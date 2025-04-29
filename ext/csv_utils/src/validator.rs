use magnus::{function, prelude::*, Error, Ruby, RArray, Symbol, Value, RHash, RModule};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::io::{Write, BufWriter, Read, BufReader};
use url::Url;
use tempfile::NamedTempFile;

// Define the chunk size threshold for processing CSV files (100MB)
const CHUNK_SIZE_BYTES: usize = 100 * 1024 * 1024;

// Define our validation rule types
enum ValidationRule {
    Ignore,       // Ignore this column
    Url,          // Validate as URL
    Protocol,     // Check if it contains ://
}

// Helper function to write a chunk of records to a temporary file in reverse order
fn write_chunk_to_temp_file(chunk: &[csv::StringRecord]) -> Result<NamedTempFile, Error> {
    let temp_file = NamedTempFile::new().map_err(|e| {
        Error::new(magnus::exception::runtime_error(), 
            format!("Failed to create temporary file: {}", e))
    })?;
    
    {
        // Write the reversed chunk to the temp file within a scope
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)  // Don't write headers for each chunk
            .from_writer(&temp_file);
        
        // Write records in reverse order to the temp file
        for record in chunk.iter().rev() {
            writer.write_record(record).map_err(|e| {
                Error::new(magnus::exception::runtime_error(), 
                    format!("Failed to write record to temp file: {}", e))
            })?;
        }
        
        writer.flush().map_err(|e| {
            Error::new(magnus::exception::runtime_error(), 
                format!("Failed to flush temp file: {}", e))
        })?;
    } // End of writer scope
    
    Ok(temp_file)
}

// Helper function to create the final reversed output file from temp files
fn create_reversed_output(
    output_path: &str, 
    headers: &[String], 
    temp_files: &[NamedTempFile]
) -> Result<(), Error> {
    // Create the final reversed output file
    let output_file = File::create(output_path).map_err(|e| {
        Error::new(magnus::exception::runtime_error(), 
            format!("Failed to create reversed output file: {}", e))
    })?;
    
    // Write headers to the output file
    let mut final_writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(&output_file);
    
    final_writer.write_record(headers).map_err(|e| {
        Error::new(magnus::exception::runtime_error(), 
            format!("Failed to write headers to reversed output: {}", e))
    })?;
    
    final_writer.flush().map_err(|e| {
        Error::new(magnus::exception::runtime_error(), 
            format!("Failed to flush reversed output: {}", e))
    })?;
    
    // Drop the final_writer to ensure it's closed
    drop(final_writer);
    
    // Open the final output file for appending
    let mut append_file = OpenOptions::new()
        .append(true)
        .open(output_path)
        .map_err(|e| {
            Error::new(magnus::exception::runtime_error(), 
                format!("Failed to open reversed output file for appending: {}", e))
        })?;
    
    // Concatenate temp files in reverse order (latest chunk first)
    for temp_file in temp_files.iter().rev() {
        let mut reader = BufReader::new(temp_file.reopen().map_err(|e| {
            Error::new(magnus::exception::runtime_error(), 
                format!("Failed to reopen temp file: {}", e))
        })?);
        
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).map_err(|e| {
            Error::new(magnus::exception::runtime_error(), 
                format!("Failed to read temp file: {}", e))
        })?;
        
        append_file.write_all(&buffer).map_err(|e| {
            Error::new(magnus::exception::runtime_error(), 
                format!("Failed to append temp file data: {}", e))
        })?;
    }
    
    // Ensure everything is written
    append_file.flush().map_err(|e| {
        Error::new(magnus::exception::runtime_error(), 
            format!("Failed to flush reversed output file: {}", e))
    })?;
    
    Ok(())
}

// Parse Ruby validation pattern to Rust validation rules
fn parse_validation_pattern(pattern: &RArray) -> Result<Vec<ValidationRule>, Error> {
    let mut rules = Vec::new();
    
    for i in 0..pattern.len() {
        // Access array elements by index directly - convert usize to isize
        let value: Value = pattern.entry(i as isize)?;
        
        if value.is_nil() {
            rules.push(ValidationRule::Ignore);
        } else if let Some(symbol) = Symbol::from_value(value) {
            let sym_str = symbol.name().map_err(|e| {
                Error::new(magnus::exception::type_error(), format!("Failed to get symbol name: {}", e))
            })?;
            match sym_str.as_ref() {
                "url" => rules.push(ValidationRule::Url),
                "protocol" => rules.push(ValidationRule::Protocol),
                _ => return Err(Error::new(magnus::exception::arg_error(), format!("Unknown validation rule: {}", sym_str))),
            }
        } else {
            return Err(Error::new(magnus::exception::type_error(), "Validation pattern must contain nil or symbols"));
        }
    }
    
    Ok(rules)
}

// Validate a CSV file with the given pattern
fn validate_csv(file_path: String, pattern: RArray, error_log_path: Option<String>, reversed_output_path: Option<String>) -> Result<RHash, Error> {
    // Parse the validation pattern
    let rules = parse_validation_pattern(&pattern)?;
    
    // Check if all rules are Ignore (nil)
    let all_rules_are_ignore = rules.iter().all(|rule| {
        match rule {
            ValidationRule::Ignore => true,
            _ => false
        }
    });
    
    // Open the CSV file
    let file_path = Path::new(&file_path);
    let file = File::open(file_path).map_err(|e| {
        Error::new(magnus::exception::runtime_error(), format!("Failed to open file: {}", e))
    })?;
    
    // Create a CSV reader
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    
    // Set up error logging if a path is provided
    let mut error_log_writer = match error_log_path {
        Some(path) => {
            let error_log_file = File::create(path).map_err(|e| {
                Error::new(magnus::exception::runtime_error(), format!("Failed to create error log file: {}", e))
            })?;
            
            let mut writer = BufWriter::new(error_log_file);
            
            // Write header (no BOM for ASCII)
            writer.write_all(b"Error Message,Row,Column\n").map_err(|e| {
                Error::new(magnus::exception::runtime_error(), format!("Failed to write header to error log: {}", e))
            })?;
            
            Some(writer)
        },
        None => None
    };
    
    // Get headers from the CSV file
    let headers: Vec<String> = match reader.headers() {
        Ok(headers) => headers.iter().map(|h| h.to_string()).collect(),
        Err(e) => {
            return Err(Error::new(magnus::exception::runtime_error(), 
                format!("Failed to read CSV headers: {}", e)))
        }
    };
    
    // Set up chunked processing for reversed output
    let need_reversed_output = reversed_output_path.is_some();
    let mut temp_files: Vec<NamedTempFile> = Vec::new();
    let mut current_chunk: Vec<csv::StringRecord> = Vec::new();
    let mut current_chunk_size: usize = 0;  // Track approximate size of current chunk in bytes
    
    // Tracking variables
    let mut row_count = 0;
    let mut errors: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    
    // Process each record
    for (row_idx, record_result) in reader.records().enumerate() {
        let record = record_result.map_err(|e| {
            Error::new(magnus::exception::runtime_error(), format!("CSV error at row {}: {}", row_idx + 1, e))
        })?;
        
        // Store the record for reversed output if needed
        if need_reversed_output {
            // Estimate the size of this record in bytes
            let record_size = record.iter().map(|field| field.len()).sum::<usize>() + record.len();
            current_chunk.push(record.clone());
            current_chunk_size += record_size;
            
            // If we've reached the chunk size threshold, process this chunk
            if current_chunk_size >= CHUNK_SIZE_BYTES {
                let temp_file = write_chunk_to_temp_file(&current_chunk)?;
                
                // Store the temp file for later use
                temp_files.push(temp_file);
                
                // Clear the current chunk for the next set of records
                current_chunk.clear();
                current_chunk_size = 0;
            }
        }
        
        row_count += 1;
        
        if record.len() < rules.len() {
            return Err(Error::new(
                magnus::exception::runtime_error(),
                format!("Row {} has fewer columns than expected (got {}, expected {})", 
                        row_idx + 1, record.len(), rules.len())
            ));
        }
        
        // If all rules are Ignore, we only needed to check that the record has enough columns
        if all_rules_are_ignore {
            // Skip the rest of the validation for this row
            continue;
        }
        
        // Track validation failures for this row
        let mut failed_url = false;
        let mut failed_protocol = false;
        
        // Validate each field according to the rules
        for (col_idx, rule) in rules.iter().enumerate() {
            // Skip if column index is out of bounds
            if col_idx >= record.len() {
                continue;
            }
            
            let field = &record[col_idx];
            
            match rule {
                ValidationRule::Ignore => {
                    // Do nothing for ignored columns
                },
                ValidationRule::Url => {
                    // Skip validation if a URL in this row already failed
                    if failed_url {
                        continue;
                    }
                    
                    // Validate URL using the url crate
                    if !field.is_empty() {
                        if let Err(_) = Url::parse(field) {
                            // Increment the error count for this rule type
                            *errors.entry("url".to_string()).or_insert(0) += 1;
                            // Mark as failed for this row
                            failed_url = true;
                            
                            // Log the error if we have a log writer
                            if let Some(writer) = &mut error_log_writer {
                                let column_name = if col_idx < headers.len() {
                                    &headers[col_idx]
                                } else {
                                    "Unknown"
                                };
                                
                                let error_line = format!("{} does not include a valid domain,{},{}\n", 
                                    field, row_idx + 1, column_name);
                                
                                writer.write_all(error_line.as_bytes()).map_err(|e| {
                                    Error::new(magnus::exception::runtime_error(), 
                                        format!("Failed to write to error log: {}", e))
                                })?;
                            }
                        }
                    }
                },
                ValidationRule::Protocol => {
                    // Skip validation if a Protocol check in this row already failed
                    if failed_protocol {
                        continue;
                    }
                    
                    // Check if field contains "://"
                    if !field.is_empty() && !field.contains("://") {
                        // Increment the error count for this rule type
                        *errors.entry("protocol".to_string()).or_insert(0) += 1;
                        // Mark as failed for this row
                        failed_protocol = true;
                        
                        // Log the error if we have a log writer
                        if let Some(writer) = &mut error_log_writer {
                            let column_name = if col_idx < headers.len() {
                                &headers[col_idx]
                            } else {
                                "Unknown"
                            };
                            
                            let error_line = format!("{} does not include a valid link protocol,{},{}\n", 
                                field, row_idx + 1, column_name);
                            
                            writer.write_all(error_line.as_bytes()).map_err(|e| {
                                Error::new(magnus::exception::runtime_error(), 
                                    format!("Failed to write to error log: {}", e))
                            })?;
                        }
                    }
                }
            }
        }
    }
    
    // Flush the error log if we have one
    if let Some(mut writer) = error_log_writer {
        writer.flush().map_err(|e| {
            Error::new(magnus::exception::runtime_error(), 
                format!("Failed to flush error log: {}", e))
        })?;
    }
    
    // If we need to write a reversed output, handle any remaining records and combine temp files
    if let Some(reversed_path) = reversed_output_path {
        // Process any remaining records in the last chunk
        if !current_chunk.is_empty() {
            let temp_file = write_chunk_to_temp_file(&current_chunk)?;
            temp_files.push(temp_file);
        }
        
        // Create the final reversed output file
        create_reversed_output(&reversed_path, &headers, &temp_files)?;
    }
    
    // Create a Ruby hash for the result
    let result = RHash::new();
    result.aset(Symbol::new("row_count"), row_count)?;
    
    // Create a Ruby hash for errors
    let errors_hash = RHash::new();
    for (key, value) in errors {
        match key.as_str() {
            "url" => errors_hash.aset(Symbol::new("url"), value)?,
            "protocol" => errors_hash.aset(Symbol::new("protocol"), value)?,
            _ => {}
        }
    }
    
    result.aset(Symbol::new("errors"), errors_hash)?;
    
    Ok(result)
}

pub fn register(ruby: &Ruby) -> Result<(), Error> {
    let class = ruby.define_class("CsvUtilsValidator", ruby.class_object())?;
    class.define_singleton_method("_validate", function!(validate_csv, 4))?;

    Ok(())
}
