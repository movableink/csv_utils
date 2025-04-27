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
use magnus::{prelude::*, Error, method, function, Ruby, Symbol, RHash, RArray, Value};
use sha1::{Sha1, Digest};

#[magnus::wrap(class = "Sorter")]
pub struct Sorter {
    inner: RefCell<SorterInner>,
}

// Inner state that can be mutated through RefCell
struct SorterInner {
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
}

// Serializable record for run files
#[derive(Serialize, Deserialize, Clone, Debug)]
struct SortRecord {
    key_hash: u64,
    key: String,
    record: Vec<String>,
}

// Key data for faster comparison
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct KeyData {
    hash: u64,
    value: String,
}

impl SorterInner {
    // Generate a composite key from row values using SHA1, joined by commas
    fn generate_targeting_key(&self, row: &[String]) -> String {
        let mut hasher = Sha1::new();
        
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

    // Hash a key for faster comparisons
    fn hash_key(key: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    // Estimate size of a row in bytes
    fn estimate_row_size(key: &KeyData, row: &[String]) -> usize {
        // Estimate key size
        let key_size = key.value.len() + std::mem::size_of::<KeyData>();
        
        // Estimate row size
        let row_size: usize = row.iter()
            .map(|s| s.len() + std::mem::size_of::<String>())
            .sum();
        
        key_size + row_size
    }
    
    // Create a sorted run file from current batch
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
    
    // Merge all temporary run files to the output file
    fn merge_runs_to_file(&mut self) -> Result<usize, std::io::Error> {
        // If no temp files, we can't merge
        if self.temp_files.is_empty() {
            return Ok(0);
        }
        
        // Create readers for each run file
        let mut readers = Vec::new();
        for temp_file in &self.temp_files {
            if let Ok(file) = File::open(temp_file.path()) {
                let mut reader = BufReader::new(file);
                
                // Try to read first record
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
        
        // Use a min-heap for efficient merging
        let mut heap = BinaryHeap::new();
        
        // Initialize the heap with first record from each reader
        for (i, (record, _)) in readers.iter().enumerate() {
            if let Some(rec) = record {
                // Use a tuple ordering to create a min-heap
                heap.push(std::cmp::Reverse((rec.key_hash, rec.key.clone(), i)));
            }
        }
        
        // Create output file
        self.output_file = NamedTempFile::new()?;
        let mut w = BufWriter::new(&self.output_file);
        let mut count = 0;
        
        // Process records in sorted order and write them directly
        while let Some(std::cmp::Reverse((_, _, src_idx))) = heap.pop() {
            if let Some((record, reader)) = readers.get_mut(src_idx) {
                if let Some(rec) = record.take() {
                    // Write record directly
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
                                // Add next record to the heap
                                heap.push(std::cmp::Reverse((next_rec.key_hash, next_rec.key.clone(), src_idx)));
                                *record = Some(next_rec);
                            }
                        }
                    }
                }
            }
        }
        
        w.flush()?;
        
        // Clear temp files
        self.temp_files.clear();
        
        Ok(count)
    }
    
    // In-memory sort and write to output file
    fn sort_in_memory_to_file(&mut self) -> Result<usize, std::io::Error> {
        // If batch is empty, no sorting needed
        if self.current_batch.is_empty() {
            return Ok(0);
        }
        
        // Sort the current batch
        self.current_batch.sort_by(|(key_a, _), (key_b, _)| {
            key_a.cmp(key_b)
        });
        
        // Collect records into a Vec to avoid borrow conflict
        let records: Vec<Vec<String>> = self.current_batch.iter()
            .map(|(_, record)| record.clone())
            .collect();
        
        // Write sorted records directly to CSV using write_records
        let total_rows = self.write_records(records)?;
        
        // Clear the batch
        self.current_batch.clear();
        self.current_buffer_size = 0;
        
        Ok(total_rows)
    }

    fn write_records<I>(&mut self, iter: I) -> io::Result<usize> where I: IntoIterator<Item = Vec<String>> {
        self.output_file = NamedTempFile::new()?;
        let mut w = BufWriter::new(&self.output_file);
        let mut count = 0;
        for rec in iter {
            // Serialize record to bytes
            let bytes = bincode::serialize(&rec)
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

    // Calculate the total memory usage of current_batch. Don't do this often as it's expensive
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
    // Create a new Sorter - helper function for constructor
    pub fn new_impl(key_columns: Vec<usize>, buffer_size_mb: Option<usize>) -> Result<Self, Error> {
        const DEFAULT_BUFFER_SIZE_MB: usize = 100;
        const DEFAULT_MAX_TARGETING_KEY_ROWS: usize = 200;

        let buffer_size_bytes = buffer_size_mb.unwrap_or(DEFAULT_BUFFER_SIZE_MB) * 1024 * 1024;
        
        // Create output file immediately
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
                key_columns,
                current_batch: Vec::new(),
                buffer_size_bytes,
                temp_files: Vec::new(),
                current_buffer_size: 0,
                output_file,
                total_rows: 0,
                observed_max_row_size: 0,
                max_targeting_key_rows: DEFAULT_MAX_TARGETING_KEY_ROWS,
            })
        })
    }
    
    // Ruby-callable constructor
    pub fn new(key_columns: Vec<usize>, buffer_size_mb: Option<usize>) -> Result<Self, Error> {
        Self::new_impl(key_columns, buffer_size_mb)
    }

    // Add a row to the sorter
    pub fn add_row(&self, row: Vec<String>) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        
        // Generate key for the row
        let key_str = inner.generate_targeting_key(&row);
        let key = KeyData {
            hash: SorterInner::hash_key(&key_str),
            value: key_str,
        };
        
        // Estimate this row's size
        let row_size = SorterInner::estimate_row_size(&key, &row);
        
        // Check if adding this row would exceed buffer size        
        if inner.current_buffer_size + row_size > inner.buffer_size_bytes && !inner.current_batch.is_empty() {
            let actual_row_data_size = inner.current_batch_size();
            inner.observed_max_row_size = inner.observed_max_row_size.max(actual_row_data_size);
            // Create a run file from current batch
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
        
        // Add row to current batch and update buffer size
        inner.current_batch.push((key, row));
        inner.current_buffer_size += row_size;
        
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
        
        // Create a Ruby hash for the result
        let result = RHash::new();
        result.aset(Symbol::new("total_rows"), total_rows)?;
        result.aset(Symbol::new("file_count"), temp_file_count)?;
        result.aset(Symbol::new("max_row_memory_usage"), inner.observed_max_row_size)?;
        Ok(result)
    }

    // Iterate over the sorted output file in batches
    pub fn each_batch(&self, batch_size: usize) -> Result<(), Error> {
        let ruby = Ruby::get().unwrap();
        let block = ruby.block_proc()?;
        let mut inner = self.inner.borrow_mut();
        
        // Seek to the beginning of the file
        if let Err(e) = inner.output_file.seek(SeekFrom::Start(0)) {
            return Err(Error::new(
                magnus::exception::runtime_error(),
                format!("Error seeking in sorted file: {}", e)
            ));
        }
        
        // Create a reader for the file
        let mut reader = BufReader::new(&inner.output_file);
        let mut current_batch: RArray = RArray::new();
        let mut last_key = String::new();
        let mut run_length = 0;
        
        // Read records from the file and yield them in batches
        loop {
            // Read length prefix (4 bytes)
            let mut len_bytes = [0u8; 4];
            if reader.read_exact(&mut len_bytes).is_err() {
                break; // EOF
            }
            let len = u32::from_le_bytes(len_bytes) as usize;
            
            // Read record bytes
            let mut bytes = vec![0u8; len];
            reader.read_exact(&mut bytes)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
            // Deserialize record
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
            
            let item = RArray::new();
            let _ = item.push(target_key.clone());
            let _ = item.push(record);
            let _ = current_batch.push(item);
            last_key = target_key;
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

pub fn register(ruby: &Ruby) -> Result<(), Error> {
    let class = ruby.define_class("Sorter", ruby.class_object())?;
    class.define_singleton_method("new", function!(Sorter::new, 2))?;
    class.define_method("add_row", method!(Sorter::add_row, 1))?;
    class.define_method("sort!", method!(Sorter::sort, 0))?;
    class.define_method("each_batch", method!(Sorter::each_batch, 1))?;
    Ok(())
}