use magnus::{prelude::*, Error, RArray, Value, Symbol, RHash, Ruby};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::PathBuf,
    collections::{BinaryHeap, HashMap},
    sync::atomic::{AtomicU64, Ordering},
    hash::{Hash, Hasher},
};
use serde::{Serialize, Deserialize};
use bincode::{serialize_into, deserialize_from};
use tempfile::NamedTempFile;
use thiserror::Error;
use csv;

static LINE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum DedupError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
    
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    
    #[error("Invalid column index: {0}")]
    InvalidColumnIndex(usize),
}

// Serializable record for run files
#[derive(Serialize, Deserialize, Clone, Debug)]
struct RunRec {
    key_hash: u64,       // Hash of the key for faster comparisons
    key: Vec<String>,
    line_no: u64,
    record: Vec<String>,
}

impl RunRec {
    // Create a new RunRec with hash calculated from key
    fn new(key: Vec<String>, line_no: u64, record: Vec<String>) -> Self {
        let key_hash = Self::hash_key(&key);
        Self { key_hash, key, line_no, record }
    }
    
    // Hash function for key values
    fn hash_key(key: &[String]) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    // Calculate the estimated memory size of this record in bytes
    fn estimated_size(&self) -> usize {
        // Calculate size of key strings
        let key_size: usize = self.key.iter()
            .map(|s| s.len() + std::mem::size_of::<String>())
            .sum();
            
        // Calculate size of record strings
        let record_size: usize = self.record.iter()
            .map(|s| s.len() + std::mem::size_of::<String>())
            .sum();
            
        // Total size is the record + key + line_no + key_hash + overhead for the struct itself
        record_size + key_size + std::mem::size_of::<u64>() * 2
    }
}

// Phase 1: Create a sorted run file from a chunk of records
fn make_run(
    reader: &mut csv::Reader<BufReader<File>>,
    key_cols: &Vec<i64>,
    buffer_size_bytes: usize,
) -> Result<Option<(NamedTempFile, PathBuf)>, DedupError> {
    // Start with a smaller initial capacity, we'll grow as needed
    let mut buffer = Vec::with_capacity(10000);
    let mut record = csv::StringRecord::new();
    let mut current_buffer_size = 0;
    
    // Fill buffer with records until we reach the size limit
    while current_buffer_size < buffer_size_bytes {
        if !reader.read_record(&mut record)? {
            break;
        }
        
        // Extract the composite key
        let key = key_cols.iter()
            .map(|&i| {
                if i >= record.len() as i64 {
                    return Err(DedupError::InvalidColumnIndex(i as usize));
                }
                Ok(record.get(i as usize).unwrap_or("").to_string())
            })
            .collect::<Result<Vec<String>, DedupError>>()?;
        
        // Use atomic counter for line numbers
        let line_no = LINE_COUNTER.fetch_add(1, Ordering::SeqCst);
        
        // Create record entry
        let entry = RunRec::new(key, line_no, record.iter().map(|s| s.to_string()).collect());
        
        // Add to buffer and update size tracker
        buffer.push(entry);
        current_buffer_size += buffer.last().unwrap().estimated_size();
    }
    
    // If no records were read, return None
    if buffer.is_empty() {
        return Ok(None);
    }
    
    // Sort buffer by key (asc), line_no (desc) - using faster unstable sort
    buffer.sort_unstable_by(|a, b| {
        // First compare by key_hash (much faster)
        match a.key_hash.cmp(&b.key_hash) {
            std::cmp::Ordering::Equal => {
                // Only if hashes are equal, compare the actual string keys
                // This handles potential (though rare) hash collisions
                a.key.cmp(&b.key)
                    .then_with(|| b.line_no.cmp(&a.line_no))
            },
            other_ordering => other_ordering,
        }
    });
    
    // Create a temporary file
    let mut temp_file = NamedTempFile::new()?;
    println!("temp_file: {:?}", temp_file.path());
    
    // Write the records to the file
    {
        let mut writer = BufWriter::new(&mut temp_file);
        for entry in &buffer {
            serialize_into(&mut writer, entry)?;
        }
        writer.flush()?;
    }
    
    // Get the path but keep the file handle
    let path = temp_file.path().to_path_buf();
    
    // Return both the file and its path
    Ok(Some((temp_file, path)))
}

// Run reader for phase 2
struct RunReader {
    reader: BufReader<File>,
    next: Option<RunRec>,
}

impl RunReader {
    fn new(path: PathBuf) -> Result<Self, std::io::Error> {
        let mut reader = BufReader::new(File::open(&path)?);
        let next = match deserialize_from::<_, RunRec>(&mut reader) {
            Ok(rec) => Some(rec),
            Err(_) => None,
        };
        
        Ok(Self { reader, next })
    }
    
    fn pop(&mut self) -> Result<Option<RunRec>, std::io::Error> {
        match deserialize_from::<_, RunRec>(&mut self.reader) {
            Ok(rec) => Ok(Some(rec)),
            Err(_) => Ok(None),
        }
    }
}

// Heap item for merging runs
struct HeapItem {
    rec: RunRec,
    src: usize,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        // First check key_hash (fast)
        if self.rec.key_hash != other.rec.key_hash {
            return false;
        }
        // Then check full equality
        self.rec.key == other.rec.key && self.rec.line_no == other.rec.line_no
    }
}

// This is a critical part - we need to create a min-heap ordered by key (ascending)
// and within each key group ordered by line_no (descending)
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary ordering: compare key_hash first (much faster)
        match self.rec.key_hash.cmp(&other.rec.key_hash) {
            std::cmp::Ordering::Equal => {
                // If hashes are equal, compare actual keys
                match self.rec.key.cmp(&other.rec.key) {
                    std::cmp::Ordering::Equal => {
                        // Secondary ordering: line_no (descending)
                        // Higher line_no (newer records) should come first
                        other.rec.line_no.cmp(&self.rec.line_no)
                    },
                    other_ordering => other_ordering,
                }
            },
            other_ordering => other_ordering,
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Phase 2: Merge runs with "top N" filtering
fn merge_runs_with_limit(
    ruby: &Ruby,
    run_paths: Vec<PathBuf>,
    output_path: &str,
    headers: &csv::StringRecord,
    keep: usize
) -> Result<usize, DedupError> {
    println!("merge_runs_with_limit");
    // Create output file and writer
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)?;
    
    // Write headers first
    writer.write_record(headers)?;
    
    // If no run files, we're done
    if run_paths.is_empty() {
        return Ok(0);
    }
    
    // Initialize readers for each run file
    let mut readers: Vec<RunReader> = Vec::new();
    for path in run_paths {
        match RunReader::new(path) {
            Ok(reader) => readers.push(reader),
            Err(e) => eprintln!("Warning: Failed to open run file: {}", e),
        }
    }
    
    // If no readers were created successfully, we're done
    if readers.is_empty() {
        return Ok(0);
    }
    println!("readers: {:?}", readers.len());
    
    // Prime the heap with the first record from each reader
    let mut heap = BinaryHeap::new();
    for (i, reader) in readers.iter_mut().enumerate() {
        if let Some(rec) = reader.next.take() {
            heap.push(HeapItem { rec, src: i });
        }
    }
    
    // Track records per key - use an LRU-style approach to limit memory usage
    // We'll use a HashMap that periodically gets cleaned
    let mut key_counts: HashMap<Vec<String>, usize> = HashMap::new();
    let mut current_key: Option<Vec<String>> = None;
    let mut total_written = 0;
    let mut cleanup_counter = 0;
    let cleanup_frequency = 100_000; // Clean up every 100K records
    
    // Process records from heap
    while let Some(HeapItem { rec, src }) = heap.pop() {
        // Check for Ruby interrupts every 10,000 records
        if total_written % 100000 == 0 {
            println!("total_written: {:?}", total_written);
            
            // This allows Ruby to handle Ctrl+C interrupts
            let _ = ruby.thread_check_ints();
        }
        
        // Check if we've moved to a new key
        let is_new_key = current_key.as_ref() != Some(&rec.key);
        if is_new_key {
            current_key = Some(rec.key.clone());
            
            // Increment cleanup counter - only do cleanup on key transitions
            cleanup_counter += 1;
            
            // Periodically clean up the key_counts map to reduce memory usage
            if cleanup_counter >= cleanup_frequency {
                cleanup_counter = 0;
                // Keep only the current key in the map, discard the rest
                let current = current_key.as_ref().unwrap().clone();
                let count = key_counts.get(&current).cloned().unwrap_or(0);
                key_counts.clear();
                key_counts.insert(current, count);
            }
        }
        
        // Get current count for this key
        let count = key_counts.entry(rec.key.clone()).or_insert(0);
        
        // Only emit if we haven't reached the limit
        if *count < keep {
            writer.write_record(&rec.record)?;
            *count += 1;
            total_written += 1;
        }
        
        // Get next record from this source and add to heap
        if let Ok(next_rec) = readers[src].pop() {
            if let Some(next) = next_rec {
                heap.push(HeapItem { rec: next, src });
            }
        }
    }
    
    writer.flush()?;
    println!("merge complete, total_written: {}", total_written);
    Ok(total_written)
}

// Main function to deduplicate a CSV file
pub fn dedupe_csv(
    ruby: &Ruby,
    input_path: String,
    output_path: String,
    key_columns: RArray,
    max_records_per_key: usize,
    buffer_size_mb: usize
) -> Result<RHash, Error> {
    // Extract key columns
    let key_cols: Vec<i64> = (0..key_columns.len())
        .filter_map(|i| {
            if let Ok(value) = key_columns.entry::<Value>(i as isize) {
                i64::try_convert(value).ok()
            } else {
                None
            }
        })
        .collect();
    
    // Convert buffer size from MB to bytes
    let buffer_size_bytes = buffer_size_mb * 1024 * 1024;
    
    // Now use the implementation function
    dedupe_csv_impl(ruby, input_path, output_path, key_cols, max_records_per_key, buffer_size_bytes)
}

// Implementation function that does the actual work
fn dedupe_csv_impl(
    ruby: &Ruby,
    input_path: String,
    output_path: String,
    key_columns: Vec<i64>,
    max_records_per_key: usize,
    buffer_size_bytes: usize
) -> Result<RHash, Error> {
    // Reset the line counter
    LINE_COUNTER.store(0, Ordering::SeqCst);
    
    // Open input file and create reader
    let file = File::open(&input_path).map_err(|e| {
        Error::new(magnus::exception::runtime_error(), 
            format!("Failed to open input file: {}", e))
    })?;
    
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(BufReader::new(file));
    
    // Get headers
    let headers = reader.headers().map_err(|e| {
        Error::new(magnus::exception::runtime_error(), 
            format!("Failed to read CSV headers: {}", e))
    })?.clone();

    if headers.is_empty() {
        return Err(Error::new(magnus::exception::runtime_error(), 
            "No headers found in input file".to_string()));
    }
    
    // Phase 1: Create sorted run files
    // Keep both the temp files (to keep them alive) and their paths
    let mut temp_files = Vec::new();
    let mut run_paths = Vec::new();
    
    loop {
        match make_run(&mut reader, &key_columns, buffer_size_bytes) {            
            Ok(Some((temp_file, path))) => {              
                run_paths.push(path);
                temp_files.push(temp_file); // Keep the file handle alive
                
                let _ = ruby.thread_check_ints();
            },
            Ok(None) => break, // No more records
            Err(e) => return Err(Error::new(
                magnus::exception::runtime_error(),
                format!("Error creating run files: {}", e)
            )),
        }
    }
    
    // Phase 2: Merge runs with top-N filtering
    let records_written = merge_runs_with_limit(
        ruby,
        run_paths.clone(), 
        &output_path, 
        &headers,
        max_records_per_key
    ).map_err(|e| {
        Error::new(magnus::exception::runtime_error(), 
            format!("Failed in merge_runs: {}", e))
    })?;
    
    // Create a Ruby hash for the result
    let result = RHash::new();
    result.aset(Symbol::new("records_written"), records_written)?;
    result.aset(Symbol::new("run_files"), run_paths.len())?;
    
    Ok(result)
}