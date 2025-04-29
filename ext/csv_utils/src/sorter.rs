use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Write, Seek, SeekFrom, Read},
    collections::BinaryHeap,
    hash::{Hash, Hasher},
    cell::RefCell,
};
use serde::{Serialize, Deserialize};
use bincode;
use tempfile::NamedTempFile;
use magnus::{prelude::*, Error, method, function, Ruby, Symbol, RHash, RArray, Value, RModule, exception::arg_error};
use sha1::{Sha1, Digest};
use crate::validator::Validator;

const DEFAULT_MAX_TARGETING_KEY_ROWS: usize = 200;

#[magnus::wrap(class = "CsvUtils::Sorter")]
pub struct Sorter {
    inner: RefCell<SorterInner>,
}

// Inner state that can be mutated through RefCell
struct SorterInner {
    source_id: String,
    key_columns: Vec<usize>,
    current_batch: Vec<(KeyData, Vec<String>)>,
    buffer_size_bytes: usize,
    temp_files: Vec<NamedTempFile>,
    current_buffer_size: usize,
    // Store the actual output file directly
    output_file: NamedTempFile,
    total_rows: usize,
    observed_max_row_size: usize,

    // Maximum number of allowed rows for a given targeting key
    max_targeting_key_rows: usize,

    validator: Option<Validator>,
}

// Serializable record for run files
#[derive(Serialize, Deserialize, Clone, Debug)]
struct SortRecord {
    key_hash: u64,
    key: String,
    record: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct KeyData {
    hash: u64,
    value: String,
}

impl SorterInner {
    // Generate a composite key from source_id + row values using SHA1, joined by commas
    fn generate_targeting_key(&self, row: &[String]) -> String {
        let mut hasher = Sha1::new();
        hasher.update(self.source_id.as_bytes());
        hasher.update(b",");
        
        for (i, &col) in self.key_columns.iter().enumerate() {
            if let Some(val) = row.get(col) {
                hasher.update(val.as_bytes());
                if i < self.key_columns.len() - 1 {
                    hasher.update(b",");
                }
            }
        }

        let digest = hasher.finalize();
        format!("{:x}", digest)
    }

    fn hash_key(key: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    fn estimate_row_size(key: &KeyData, row: &[String]) -> usize {
        let key_size = key.value.len() + std::mem::size_of::<KeyData>();
        let row_size: usize = row.iter()
            .map(|s| s.len() + std::mem::size_of::<String>())
            .sum();
        
        key_size + row_size
    }
    
    fn make_run(&mut self) -> std::io::Result<Option<NamedTempFile>> {
        if self.current_batch.is_empty() {
            return Ok(None);
        }

        self.current_batch.sort_unstable();
        let temp = NamedTempFile::new()?;
        {
            let mut w = BufWriter::new(&temp);
            for (key, rec) in &self.current_batch {
                let sort_record = SortRecord {
                    key_hash: key.hash,
                    key: key.value.clone(),
                    record: rec.clone(),
                };
                let bytes = bincode::serialize(&sort_record)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                w.write_all(&bytes)?;
            }
            w.flush()?;
        }

        self.current_batch.clear();
        self.current_buffer_size = 0;
        Ok(Some(temp))
    }
    
    fn merge_runs_to_file(&mut self) -> Result<usize, std::io::Error> {
        if self.temp_files.is_empty() {
            return Ok(0);
        }
        
        let mut readers = Vec::new();
        for temp_file in &self.temp_files {
            if let Ok(file) = File::open(temp_file.path()) {
                let mut reader = BufReader::new(file);
                
                let mut len_bytes = [0u8; 4];
                if reader.read_exact(&mut len_bytes).is_ok() {
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    let mut bytes = vec![0u8; len];
                    if reader.read_exact(&mut bytes).is_ok() {
                        if let Ok(record) = bincode::deserialize::<SortRecord>(&bytes) {
                            readers.push((Some(record), reader));
                        }
                    }
                }
            }
        }
        
        let mut heap = BinaryHeap::new();
        
        // Initialize the heap with first record from each reader
        for (i, (record, _)) in readers.iter().enumerate() {
            if let Some(rec) = record {
                // Use a tuple ordering to create a min-heap
                heap.push(std::cmp::Reverse((rec.key_hash, rec.key.clone(), i)));
            }
        }
        
        self.output_file = NamedTempFile::new()?;
        let mut w = BufWriter::new(&self.output_file);
        let mut count = 0;
        
        while let Some(std::cmp::Reverse((_, _, src_idx))) = heap.pop() {
            if let Some((record, reader)) = readers.get_mut(src_idx) {
                if let Some(rec) = record.take() {
                    let bytes = bincode::serialize(&rec.record)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                    w.write_all(&bytes)?;
                    count += 1;
                    
                    // Read next record from this source
                    let mut len_bytes = [0u8; 4];
                    if reader.read_exact(&mut len_bytes).is_ok() {
                        let len = u32::from_le_bytes(len_bytes) as usize;
                        let mut bytes = vec![0u8; len];
                        if reader.read_exact(&mut bytes).is_ok() {
                            if let Ok(next_rec) = bincode::deserialize::<SortRecord>(&bytes) {
                                heap.push(std::cmp::Reverse((next_rec.key_hash, next_rec.key.clone(), src_idx)));
                                *record = Some(next_rec);
                            }
                        }
                    }
                }
            }
        }
        
        w.flush()?;
        
        self.temp_files.clear();
        
        Ok(count)
    }
    
    fn sort_in_memory_to_file(&mut self) -> Result<usize, std::io::Error> {
        if self.current_batch.is_empty() {
            return Ok(0);
        }
        
        self.current_batch.sort_by(|(key_a, _), (key_b, _)| {
            key_a.cmp(key_b)
        });
        
        // Write sorted records directly to CSV using write_records
        let total_rows = self.write_records()?;        
        
        // Clear the batch
        self.current_batch.clear();
        self.current_buffer_size = 0;
        
        Ok(total_rows)
    }

    fn write_records(&mut self) -> io::Result<usize> {
        self.output_file = NamedTempFile::new()?;
        let mut w = BufWriter::new(&self.output_file);
        let mut count = 0;
        for rec in self.current_batch.iter() {
            // Serialize record to bytes
            let bytes = bincode::serialize(&rec.1)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            
            // Write length as u32 (4 bytes)
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            // Write the record bytes
            w.write_all(&bytes)?;
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
        let elements_size: usize = self.current_batch.iter()
            .map(|(key, row)| {
                // Size of KeyData struct
                let key_size = std::mem::size_of_val(key) + key.value.capacity();
                
                // Size of Vec<String> and its contents
                let row_size = std::mem::size_of_val(row) + 
                    row.iter().map(|s| s.capacity()).sum::<usize>();
                
                key_size + row_size
            })
            .sum();
        
        vec_size + elements_size
    }
}

impl Sorter {
    pub fn new(source_id: String, key_columns: Vec<usize>, buffer_size_mb: usize) -> Result<Self, Error> {
        let buffer_size_bytes = buffer_size_mb * 1024 * 1024;
        
        let output_file = match NamedTempFile::new() {
            Ok(file) => file,
            Err(e) => {
                return Err(Error::new(
                    magnus::exception::runtime_error(),
                    format!("Failed to create output file: {}", e)
                ));
            }
        };
        
        Ok(Self { 
            inner: RefCell::new(SorterInner {
                source_id,
                key_columns,
                current_batch: Vec::new(),
                buffer_size_bytes,
                temp_files: Vec::new(),
                current_buffer_size: 0,
                output_file,
                total_rows: 0,
                observed_max_row_size: 0,
                max_targeting_key_rows: DEFAULT_MAX_TARGETING_KEY_ROWS,
                validator: None,
            })
        })
    }

    pub fn set_validation_schema(&self, schema: RArray) -> Result<(), Error> {
        let mut pattern = Vec::new();
        for i in 0..schema.len() {
          let value: Value = schema.entry(i as isize)?;
          if let Some(symbol) = Symbol::from_value(value) {
            pattern.push(symbol.to_string());
          } else if value.is_nil() {
            pattern.push(String::new());
          } else {
            return Err(Error::new(arg_error(), "Pattern must be an array of symbols"));
          }
        }

        let mut inner = self.inner.borrow_mut();
        inner.validator = Some(Validator::new(pattern).map_err(|e| 
            Error::new(magnus::exception::arg_error(), e.to_string()))?
        );
        
        Ok(())
    }

    pub fn add_row(&self, row: Vec<String>) -> bool {
        let mut inner = self.inner.borrow_mut();
        
        let key_str = inner.generate_targeting_key(&row);
        let key = KeyData {
            hash: SorterInner::hash_key(&key_str),
            value: key_str,
        };
        
        let row_size = SorterInner::estimate_row_size(&key, &row);
        
        // Check if adding this row would exceed buffer size        
        if inner.current_buffer_size + row_size > inner.buffer_size_bytes && !inner.current_batch.is_empty() {
            let actual_row_data_size = inner.current_batch_size();
            inner.observed_max_row_size = inner.observed_max_row_size.max(actual_row_data_size);

            // Create a new run file from current batch
            match inner.make_run() {
                Ok(Some(run_file)) => {
                    inner.temp_files.push(run_file);
                },
                Ok(None) => {
                    // No records to process, this is fine
                },
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
        
        inner.current_batch.push((key, row));
        inner.current_buffer_size += row_size;
        
        true
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
                },
                Err(e) => {
                    return Err(Error::new(
                        magnus::exception::runtime_error(),
                        format!("Error sorting data: {}", e)
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
                    },
                    Ok(None) => {
                        // No records to process, this is fine
                    },
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
                },
                Err(e) => {
                    return Err(Error::new(
                        magnus::exception::runtime_error(),
                        format!("Error merging data: {}", e)
                    ));
                }
            }
        };
        
        let result = RHash::new();
        result.aset(Symbol::new("total_rows"), total_rows)?;
        result.aset(Symbol::new("file_count"), temp_file_count)?;
        result.aset(Symbol::new("max_row_memory_usage"), inner.observed_max_row_size)?;

        if let Some(validator) = &inner.validator {
            result.aset(Symbol::new("total_rows_processed"), validator.total_rows)?;
            result.aset(Symbol::new("failed_url_error_count"), validator.failed_url_error_count)?;
            result.aset(Symbol::new("failed_protocol_error_count"), validator.failed_protocol_error_count)?;
        }

        Ok(result)
    }

    // Iterate over the sorted output file in batches
    pub fn each_batch(&self, batch_size: usize) -> Result<(), Error> {
        let ruby = Ruby::get().unwrap();
        let block = ruby.block_proc()?;
        let mut inner = self.inner.borrow_mut();
        
        if let Err(e) = inner.output_file.seek(SeekFrom::Start(0)) {
            return Err(Error::new(
                magnus::exception::runtime_error(),
                format!("Error seeking in sorted file: {}", e)
            ));
        }
        
        let mut reader = BufReader::new(&inner.output_file);
        let mut current_batch: RArray = RArray::new();
        let mut last_key = String::new();
        let mut run_length = 0;
        
        loop {
            let mut len_bytes = [0u8; 4];
            if reader.read_exact(&mut len_bytes).is_err() {
                break; // EOF
            }
            let len = u32::from_le_bytes(len_bytes) as usize;
            
            let mut bytes = vec![0u8; len];
            reader.read_exact(&mut bytes)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
            let record: Vec<String> = bincode::deserialize(&bytes)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
            let target_key = inner.generate_targeting_key(&record);            

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
            if current_batch.len() >= batch_size {
                if target_key != last_key {
                    let args = RArray::new();
                    let _ = args.push(current_batch);
                    block.call::<_, Value>(args)?;
                    current_batch = RArray::new();
                }
            }
            
            last_key = target_key.clone();
            
            let item = RArray::new();
            let _ = item.push(target_key);
            let _ = item.push(record);
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
}

pub fn register(ruby: &Ruby, module: &RModule) -> Result<(), Error> {
    let class = module.define_class("Sorter", ruby.class_object())?;
    class.define_singleton_method("new", function!(Sorter::new, 3))?;
    class.define_method("set_validation_schema", method!(Sorter::set_validation_schema, 1))?;
    class.define_method("add_row", method!(Sorter::add_row, 1))?;
    class.define_method("sort!", method!(Sorter::sort, 0))?;
    class.define_method("each_batch", method!(Sorter::each_batch, 1))?;    

    Ok(())
}