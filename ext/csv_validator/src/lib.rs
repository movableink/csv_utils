use magnus::{function, prelude::*, Error, Ruby, RArray, Symbol, Value, RHash};
use std::fs::File;
use std::path::Path;
use std::io::{Write, BufWriter};
use url::Url;

// Define our validation rule types
enum ValidationRule {
    Ignore,       // Ignore this column
    Url,          // Validate as URL
    Protocol,     // Check if it contains ://
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
fn validate_csv(file_path: String, pattern: RArray, error_log_path: Option<String>) -> Result<RHash, Error> {
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
    
    // Tracking variables
    let mut row_count = 0;
    let mut errors: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    
    // Process each record
    for (row_idx, record_result) in reader.records().enumerate() {
        let record = record_result.map_err(|e| {
            Error::new(magnus::exception::runtime_error(), format!("CSV error at row {}: {}", row_idx + 1, e))
        })?;
        
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

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("CsvValidator")?;
    module.define_singleton_method("_validate", function!(validate_csv, 3))?;
    Ok(())
}