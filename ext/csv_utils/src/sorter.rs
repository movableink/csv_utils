use crate::postgres_copier::{GeoIndexes, PostgresCopier};
use crate::validator::{ruby_rules_array_to_rules, Validator};
use bincode::{Decode, Encode};
use magnus::{function, method, prelude::*, Error, RArray, RHash, RModule, Ruby, Symbol, Value};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    cell::RefCell,
    collections::BinaryHeap,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::Path,
};
use tempfile::tempfile;

const BUFFER_CAPACITY: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_TARGETING_KEY_ROWS: usize = 200;

#[magnus::wrap(class = "CsvUtils::Sorter")]
pub struct Sorter {
    inner: RefCell<SorterInner>,
}

// Inner state that can be mutated through RefCell
struct SorterInner {
    source_id: String,
    source_key: String,
    key_columns: Vec<usize>,
    geo_columns: Option<GeoIndexes>,
    current_batch: Vec<SortRecord>,
    buffer_size_bytes: usize,
    temp_files: Vec<File>,
    current_buffer_size: usize,
    // Store the actual output file directly
    output_file: File,
    total_rows: usize,
    observed_max_row_size: usize,

    // Maximum number of allowed rows for a given targeting key
    max_targeting_key_rows: usize,

    validator: Option<Validator>,
    buf: Vec<u8>,
}

// Serializable record for run files
#[derive(Encode, Decode, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct SortRecord {
    pub key: KeyData,
    pub record: Vec<String>,
}

impl Ord for SortRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.key).cmp(&(other.key))
    }
}

// Implementation needs to match the Ord implementation
impl PartialOrd for SortRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Encode, Decode, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KeyData {
    pub value: [u8; 20],
    pub position: usize,
}

impl Ord for KeyData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare: hash, value, reverse position
        (&self.value, std::cmp::Reverse(self.position))
            .cmp(&(&other.value, std::cmp::Reverse(other.position)))
    }
}

impl PartialOrd for KeyData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl SorterInner {
    // Generate a composite key from source_id + row values using SHA1, joined by commas
    fn generate_targeting_key(&self, row: &[String]) -> [u8; 20] {
        let mut hasher = Sha1::new();
        hasher.update(self.source_id.as_bytes());

        for &col in self.key_columns.iter() {
            if let Some(val) = row.get(col) {
                hasher.update(b",");
                hasher.update(val.as_bytes());
            }
        }

        hasher.finalize().into()
    }

    fn estimate_row_size(row: &[String]) -> usize {
        let key_size = std::mem::size_of::<KeyData>();
        let row_size: usize = row.iter().map(|s| s.len()).sum();

        key_size + row_size
    }

    fn make_run(&mut self) -> std::io::Result<Option<File>> {
        if self.current_batch.is_empty() {
            return Ok(None);
        }

        // Sort in place before taking ownership
        self.current_batch.sort_unstable();

        let temp = tempfile()?;
        {
            let mut w = BufWriter::with_capacity(BUFFER_CAPACITY, &temp);

            for sort_record in self.current_batch.drain(..) {
                // First, write the key data
                bincode::encode_into_std_write(&sort_record.key, &mut w, bincode::config::legacy())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                // Write bincode into a buffer so we can record the size of the record
                self.buf.clear();
                let length = bincode::encode_into_std_write(
                    &sort_record,
                    &mut self.buf,
                    bincode::config::legacy(),
                )
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                // [key] [size of record] [record bytes] will make it easy to read the record back in later
                w.write_all(&(length as u32).to_le_bytes())?;
                w.write_all(&self.buf)?;
            }
            w.flush()?;
        }

        // current_batch is now empty due to drain, no need to clear
        self.current_buffer_size = 0;
        Ok(Some(temp))
    }

    fn merge_runs_to_file(&mut self) -> Result<usize, std::io::Error> {
        if self.temp_files.is_empty() {
            return Ok(0);
        }

        // Allocate a buffer for the key data of the size of the KeyData struct
        let mut key_bytes = bincode::encode_to_vec(
            &KeyData {
                value: [0u8; 20],
                position: 0,
            },
            bincode::config::legacy(),
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Prepare readers with their first records
        let mut readers = Vec::with_capacity(self.temp_files.len());
        for file in &self.temp_files {
            let mut reader = BufReader::with_capacity(BUFFER_CAPACITY, file);

            if reader.read_exact(&mut key_bytes).is_err() {
                continue;
            }

            let (key, _): (KeyData, usize) =
                bincode::decode_from_slice(&key_bytes, bincode::config::legacy())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let mut record_length_bytes = [0u8; 4];
            reader.read_exact(&mut record_length_bytes)?;

            let record_length = u32::from_le_bytes(record_length_bytes) as usize;
            let mut record_bytes = vec![0u8; record_length];
            reader.read_exact(&mut record_bytes)?;

            readers.push((key, record_bytes, reader));
        }

        // Create min-heap for merge sorting
        let mut heap = BinaryHeap::with_capacity(readers.len());
        for (i, (key, _, _)) in readers.iter().enumerate() {
            heap.push((key.clone(), i));
        }

        // Prepare output file
        self.output_file.rewind()?;
        let mut writer = BufWriter::with_capacity(BUFFER_CAPACITY, &self.output_file);
        let mut count = 0;

        // Process records in sorted order
        while let Some((_, src_idx)) = heap.pop() {
            if let Some((key, record_bytes, reader)) = readers.get_mut(src_idx) {
                // Write current record
                writer.write_all(&(record_bytes.len() as u32).to_le_bytes())?;
                writer.write_all(record_bytes)?;
                count += 1;

                if reader.read_exact(&mut key_bytes).is_err() {
                    // No more records in this reader
                    continue;
                }

                let (next_key, _): (KeyData, usize) =
                    bincode::decode_from_slice(&key_bytes, bincode::config::legacy())
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                let mut record_length_bytes = [0u8; 4];
                reader.read_exact(&mut record_length_bytes)?;

                let record_length = u32::from_le_bytes(record_length_bytes) as usize;
                let mut rec_bytes = vec![0u8; record_length];
                reader.read_exact(&mut rec_bytes)?;
                heap.push((next_key.clone(), src_idx));
                *key = next_key;
                *record_bytes = rec_bytes;
            }
        }

        writer.flush()?;
        self.temp_files.clear();

        Ok(count)
    }

    fn sort_in_memory_to_file(&mut self) -> Result<usize, std::io::Error> {
        if self.current_batch.is_empty() {
            return Ok(0);
        }

        self.current_batch.sort_unstable();

        // Write sorted records directly to CSV using write_records
        let total_rows = self.write_records()?;

        // Clear the batch
        self.current_batch.clear();
        self.current_buffer_size = 0;

        Ok(total_rows)
    }

    fn write_records(&mut self) -> io::Result<usize> {
        self.output_file.rewind()?;
        let mut w = BufWriter::with_capacity(BUFFER_CAPACITY, &self.output_file);
        let mut count = 0;
        for rec in self.current_batch.iter() {
            // Serialize record to bytes
            self.buf.clear();
            let length =
                bincode::encode_into_std_write(rec, &mut self.buf, bincode::config::legacy())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            // Write length as u32 (4 bytes)
            w.write_all(&(length as u32).to_le_bytes())?;
            // Write the record bytes
            w.write_all(&self.buf)?;
            count += 1;
        }
        w.flush()?;
        Ok(count)
    }

    // total memory usage of current_batch (expensive to call)
    fn current_batch_size(&self) -> usize {
        // Size of the Vec itself
        let vec_size = std::mem::size_of_val(&self.current_batch);

        // Size of each element in the Vec
        let elements_size: usize = self
            .current_batch
            .iter()
            .map(|sort_record| {
                // Size of KeyData struct
                let key_size =
                    std::mem::size_of_val(&sort_record.key) + std::mem::size_of::<[u8; 20]>();

                // Size of Vec<String> and its contents
                let row_size = std::mem::size_of_val(&sort_record.record)
                    + sort_record
                        .record
                        .iter()
                        .map(|s| s.capacity())
                        .sum::<usize>();

                key_size + row_size
            })
            .sum();

        vec_size + elements_size
    }
}

impl Sorter {
    pub fn new(
        source_id: String,
        source_key: String,
        key_columns: Vec<usize>,
        geo_columns_vec: Option<Vec<usize>>,
        buffer_size_mb: usize,
    ) -> Result<Self, Error> {
        let buffer_size_bytes = buffer_size_mb * 1024 * 1024;

        let geo_columns = geo_columns_vec.map(|indexes| (indexes[0], indexes[1]));

        let output_file = tempfile().map_err(|e| {
            Error::new(
                magnus::exception::runtime_error(),
                format!("Failed to create output file: {}", e),
            )
        })?;

        Ok(Self {
            inner: RefCell::new(SorterInner {
                source_id,
                source_key,
                key_columns,
                geo_columns,
                current_batch: Vec::new(),
                buffer_size_bytes,
                temp_files: Vec::new(),
                current_buffer_size: 0,
                output_file,
                total_rows: 0,
                observed_max_row_size: 0,
                max_targeting_key_rows: DEFAULT_MAX_TARGETING_KEY_ROWS,
                validator: None,
                buf: Vec::with_capacity(BUFFER_CAPACITY),
            }),
        })
    }

    pub fn enable_validation(&self, schema: RArray, error_log_path: String) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        let rules = ruby_rules_array_to_rules(schema)
            .map_err(|e| Error::new(magnus::exception::arg_error(), e.to_string()))?;
        inner.validator = Some(
            Validator::new(rules, error_log_path)
                .map_err(|e| Error::new(magnus::exception::arg_error(), e.to_string()))?,
        );

        Ok(())
    }

    pub fn add_row(&self, row: Vec<String>, position: usize) -> bool {
        let mut inner = self.inner.borrow_mut();

        let key_bytes = inner.generate_targeting_key(&row);
        let key = KeyData {
            value: key_bytes,
            position,
        };

        let row_size = SorterInner::estimate_row_size(&row);

        // Check if adding this row would exceed buffer size
        if inner.current_buffer_size + row_size > inner.buffer_size_bytes
            && !inner.current_batch.is_empty()
        {
            let actual_row_data_size = inner.current_batch_size();
            inner.observed_max_row_size = inner.observed_max_row_size.max(actual_row_data_size);

            // Create a new run file from current batch
            match inner.make_run() {
                Ok(Some(run_file)) => {
                    inner.temp_files.push(run_file);
                }
                Ok(None) => {
                    // No records to process, this is fine
                }
                Err(e) => {
                    eprintln!("Error creating run file: {}", e);
                }
            }
        }

        if let Some(validator) = &mut inner.validator {
            if validator.validate_row(&row) {
                return false;
            }
        }

        inner.current_batch.push(SortRecord { key, record: row });
        inner.current_buffer_size += row_size;

        true
    }

    pub fn add_file(&self, file_path: String) -> Result<(), Error> {
        // parse csv file, skipping headers
        let file = File::open(file_path)
            .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
        let mut reader = csv::Reader::from_reader(file);
        // Allocate a buffer for the record
        let mut record = csv::StringRecord::new();
        let mut position = 0;

        loop {
            match reader.read_record(&mut record) {
                Ok(true) => {
                    // Convert ByteRecord to Vec<String>
                    let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();

                    self.add_row(row, position);
                    position += 1;
                }
                Ok(false) => break, // End of file
                Err(e) => {
                    if let Some(validator) = &mut self.inner.borrow_mut().validator {
                        let _ = validator.add_error_to_file("parse", 0, 0, &e.to_string());
                        validator.parse_error_count += 1;
                    }
                    position += 1;
                    // Continue processing other records
                }
            }
        }

        Ok(())
    }

    // Sort all rows and write to a final temp file, return total rows information
    pub fn sort(&self) -> Result<RHash, Error> {
        let mut inner = self.inner.borrow_mut();
        let temp_file_count = inner.temp_files.len();

        let actual_row_data_size = inner.current_batch_size();
        inner.observed_max_row_size = inner.observed_max_row_size.max(actual_row_data_size);

        // If there are no temp files and only data in current batch, sort in memory
        let total_rows = if inner.temp_files.is_empty() && !inner.current_batch.is_empty() {
            match inner.sort_in_memory_to_file() {
                Ok(count) => {
                    inner.total_rows = count;
                    count
                }
                Err(e) => {
                    return Err(Error::new(
                        magnus::exception::runtime_error(),
                        format!("Error sorting data: {}", e),
                    ));
                }
            }
        } else {
            // Otherwise we need to create a run from any remaining records
            // and merge all runs
            if !inner.current_batch.is_empty() {
                match inner.make_run() {
                    Ok(Some(run_file)) => {
                        inner.temp_files.push(run_file);
                    }
                    Ok(None) => {
                        // No records to process, this is fine
                    }
                    Err(e) => {
                        eprintln!("Error creating run file: {}", e);
                    }
                }
            }

            // Merge all runs to a final file
            match inner.merge_runs_to_file() {
                Ok(count) => {
                    inner.total_rows = count;
                    count
                }
                Err(e) => {
                    return Err(Error::new(
                        magnus::exception::runtime_error(),
                        format!("Error merging data: {}", e),
                    ));
                }
            }
        };

        let result = RHash::new();
        result.aset(Symbol::new("total_rows"), total_rows)?;
        result.aset(Symbol::new("file_count"), temp_file_count)?;
        result.aset(
            Symbol::new("max_row_memory_usage"),
            inner.observed_max_row_size,
        )?;

        if let Some(validator) = &inner.validator {
            result.aset(Symbol::new("total_rows_processed"), validator.total_rows)?;
            result.aset(
                Symbol::new("failed_url_error_count"),
                validator.failed_url_error_count,
            )?;
            result.aset(
                Symbol::new("failed_protocol_error_count"),
                validator.failed_protocol_error_count,
            )?;
            result.aset(
                Symbol::new("parse_error_count"),
                validator.parse_error_count,
            )?;
            result.aset(
                Symbol::new("error_count"),
                validator.failed_url_error_count
                    + validator.failed_protocol_error_count
                    + validator.parse_error_count,
            )?;
        } else {
            result.aset(Symbol::new("error_count"), 0)?;
        }

        Ok(result)
    }

    // Iterate over the sorted output file in batches
    pub fn each_batch(&self, batch_size: usize) -> Result<(), Error> {
        let ruby = Ruby::get().unwrap();
        let block = ruby.block_proc()?;
        let mut inner = self.inner.borrow_mut();

        inner.output_file.rewind().map_err(|e| {
            Error::new(
                magnus::exception::runtime_error(),
                format!("Error seeking in sorted file: {}", e),
            )
        })?;

        let mut reader = BufReader::with_capacity(BUFFER_CAPACITY, &inner.output_file);
        let mut current_batch: RArray = RArray::new();
        let mut last_key = [0u8; 20];
        let mut run_length = 0;

        loop {
            let mut len_bytes = [0u8; 4];
            if reader.read_exact(&mut len_bytes).is_err() {
                break; // EOF
            }
            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut bytes = vec![0u8; len];
            reader
                .read_exact(&mut bytes)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;

            let record: SortRecord = bincode::decode_from_slice(&bytes, bincode::config::legacy())
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?
                .0;

            let target_key = record.key.value;

            if !current_batch.is_empty() && target_key == last_key {
                run_length += 1;
            } else {
                run_length = 1;
            }

            if run_length > inner.max_targeting_key_rows {
                // We will never serve more than MAX_RUN_LENGTH rows for a given key, so
                // may as well not emit them
                continue;
            }

            // If the batch is full, complete the target_key run and then start a new batch
            if current_batch.len() >= batch_size && target_key != last_key {
                let args = RArray::new();
                let _ = args.push(current_batch);
                block.call::<_, Value>(args)?;
                current_batch = RArray::new();
            }

            last_key = target_key;

            let item = RArray::new();
            let key_hex = target_key
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();
            let _ = item.push(key_hex);
            let _ = item.push(record.record);
            let _ = current_batch.push(item);
        }

        // Yield any remaining records
        if !current_batch.is_empty() {
            let args = RArray::new();
            let _ = args.push(current_batch);
            block.call::<_, Value>(args)?;
        }

        Ok(())
    }

    pub fn write_binary_postgres_file(&self, file_path: String) -> Result<(), Error> {
        let inner = self.inner.borrow_mut();
        let output_file_path = Path::new(&file_path);

        // Input comes from previous step's output file
        let mut input_file = inner.output_file.try_clone().map_err(|e| {
            Error::new(
                magnus::exception::runtime_error(),
                format!("Failed to clone output file: {}", e),
            )
        })?;
        input_file.rewind().map_err(|e| {
            Error::new(
                magnus::exception::runtime_error(),
                format!("Failed to rewind input file: {}", e),
            )
        })?;

        let mut copier =
            PostgresCopier::new(input_file, inner.geo_columns, inner.source_key.clone())
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;

        copier
            .copy(output_file_path)
            .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;

        Ok(())
    }
}

pub fn register(ruby: &Ruby, module: &RModule) -> Result<(), Error> {
    let class = module.define_class("Sorter", ruby.class_object())?;
    class.define_singleton_method("new", function!(Sorter::new, 5))?;
    class.define_method("enable_validation", method!(Sorter::enable_validation, 2))?;
    class.define_method("add_row", method!(Sorter::add_row, 2))?;
    class.define_method("add_file", method!(Sorter::add_file, 1))?;
    class.define_method("sort!", method!(Sorter::sort, 0))?;
    class.define_method("each_batch", method!(Sorter::each_batch, 1))?;
    class.define_method(
        "write_binary_postgres_file",
        method!(Sorter::write_binary_postgres_file, 1),
    )?;

    Ok(())
}
