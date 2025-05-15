use magnus::{
    exception::arg_error, function, method, prelude::*, Error, RArray, RHash, RModule, Ruby,
    Symbol, Value,
};
use std::cell::RefCell;
use std::error::Error as StdError;
use std::fmt;
use std::fs::File;
use std::io::Write;
use url::Url;

#[derive(Debug)]
pub struct ValidationError {
    message: String,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl StdError for ValidationError {}

#[derive(Debug)]
pub struct ValidationRule {
    column_name: String,
    validation_type: ValidationType,
}

#[derive(Debug)]
enum ValidationType {
    Ignore,   // Ignore this column
    Url,      // Validate as URL
    Protocol, // Check if it contains ://
    Invalid,  // Invalid validation type
}

impl ValidationType {
    fn from_string(s: &str) -> Self {
        match s {
            "" => ValidationType::Ignore,
            "ignore" => ValidationType::Ignore,
            "url" => ValidationType::Url,
            "protocol" => ValidationType::Protocol,
            _ => ValidationType::Invalid,
        }
    }
}

pub struct Validator {
    rules: Vec<ValidationRule>,
    error_log_file: Option<File>,
    pub total_rows: usize,
    pub failed_url_error_count: usize,
    pub failed_protocol_error_count: usize,
    pub parse_error_count: usize,
    pub first_error_row: Option<usize>,
    first_error_type: Option<ValidationType>,
}

impl Validator {
    pub fn new(
        rules: Vec<ValidationRule>,
        error_log_path: String,
    ) -> Result<Self, ValidationError> {
        // Create error log file with BOM
        let error_log_file = match File::create(&error_log_path) {
            Ok(mut file) => {
                // Write UTF-8 BOM
                if let Err(e) = file.write_all(b"\xEF\xBB\xBF") {
                    return Err(ValidationError {
                        message: format!("Failed to write BOM: {}", e),
                    });
                }
                // Write header
                if let Err(e) = writeln!(file, "Error Message,Row,Column") {
                    return Err(ValidationError {
                        message: format!("Failed to write header: {}", e),
                    });
                }
                Some(file)
            }
            Err(e) => {
                return Err(ValidationError {
                    message: format!("Failed to create error log file: {}", e),
                })
            }
        };

        Ok(Self {
            rules,
            error_log_file,
            total_rows: 0,
            failed_url_error_count: 0,
            failed_protocol_error_count: 0,
            parse_error_count: 0,
            first_error_row: None,
            first_error_type: None,
        })
    }

    pub fn add_error_to_file(
        &mut self,
        error_type: &str,
        row_number: usize,
        column: usize,
        column_name: &str,
    ) -> Result<(), ValidationError> {
        if self.failed_url_error_count > 5000
            || self.failed_protocol_error_count > 5000
            || self.parse_error_count > 5000
        {
            // Stop logging errors if we have too many
            return Ok(());
        }

        if self.first_error_row.is_none() {
            self.first_error_row = Some(row_number);
            self.first_error_type = Some(ValidationType::from_string(error_type));
        }

        if let Some(file) = &mut self.error_log_file {
            let message = match error_type {
                "protocol" => format!("{} does not include a valid link protocol", column_name),
                "url" => format!("{} does not include a valid domain", column_name),
                _ => {
                    return Err(ValidationError {
                        message: format!("Unknown error type: {}", error_type),
                    })
                }
            };

            if let Err(e) = writeln!(file, "{},{},{}", message, row_number + 1, column + 1) {
                return Err(ValidationError {
                    message: format!("Failed to write error to log: {}", e),
                });
            }
        }
        Ok(())
    }

    pub fn validate_row(&mut self, row: &[String]) -> bool {
        let mut failed_url = false;
        let mut failed_protocol = false;
        let mut errors_to_log = Vec::new();

        for (col_idx, rule) in self.rules.iter().enumerate() {
            let field = &row[col_idx];

            match rule.validation_type {
                ValidationType::Invalid => continue,
                ValidationType::Ignore => continue,
                ValidationType::Url => {
                    if !field.is_empty() && Url::parse(field).is_err() {
                        failed_url = true;
                        errors_to_log.push(("url", col_idx, rule.column_name.clone()));
                    }
                }
                ValidationType::Protocol => {
                    if !field.is_empty() && !field.contains("://") {
                        failed_protocol = true;
                        errors_to_log.push(("protocol", col_idx, rule.column_name.clone()));
                    }
                }
            }
        }

        // Log all errors after validation is complete
        for (error_type, col_idx, value) in errors_to_log {
            if let Err(e) = self.add_error_to_file(error_type, self.total_rows, col_idx, &value) {
                eprintln!("Failed to log {} validation error: {}", error_type, e);
            }
        }

        if failed_url {
            self.failed_url_error_count += 1;
        }

        if failed_protocol {
            self.failed_protocol_error_count += 1;
        }

        self.total_rows += 1;

        !failed_url && !failed_protocol
    }

    pub fn first_error_message(&self) -> Option<String> {
        match self.first_error_type {
            Some(ValidationType::Url) => Some(format!(
                "Invalid image URL: {}",
                self.first_error_row.unwrap()
            )),
            Some(ValidationType::Protocol) => {
                Some(format!("Invalid link: {}", self.first_error_row.unwrap()))
            }
            Some(ValidationType::Invalid) => Some(format!(
                "Error parsing row: {}",
                self.first_error_row.unwrap() + 1
            )),
            _ => None,
        }
    }

    pub fn status(&self) -> Result<RHash, Error> {
        let status = RHash::new();
        status.aset(Symbol::new("total_rows_processed"), self.total_rows)?;
        status.aset(
            Symbol::new("failed_url_error_count"),
            self.failed_url_error_count,
        )?;
        status.aset(
            Symbol::new("failed_protocol_error_count"),
            self.failed_protocol_error_count,
        )?;
        status.aset(Symbol::new("parse_error_count"), self.parse_error_count)?;
        status.aset(
            Symbol::new("error_count"),
            self.failed_url_error_count + self.failed_protocol_error_count + self.parse_error_count,
        )?;
        if let Some(first_error_row) = self.first_error_row {
            status.aset(Symbol::new("first_error_row"), first_error_row)?;
        }
        if let Some(message) = self.first_error_message() {
            status.aset(Symbol::new("first_error_message"), message)?;
        }

        Ok(status)
    }
}

pub fn ruby_rules_array_to_rules(rules: RArray) -> Result<Vec<ValidationRule>, Error> {
    let validation_type_key = Symbol::new("validation_type");
    let column_name_key = Symbol::new("column_name");
    rules
        .into_iter()
        .map(|rule| {
            let rule = RHash::try_convert(rule)?;
            let column_name = rule
                .aref::<Symbol, Value>(column_name_key)
                .map_err(|_| Error::new(arg_error(), "Missing column_name"))?
                .to_string();
            let validation_type_str = rule
                .aref::<Symbol, Value>(validation_type_key)
                .map_err(|_| Error::new(arg_error(), "Missing validation_type"))?
                .to_string();

            match ValidationType::from_string(validation_type_str.as_str()) {
                ValidationType::Invalid => Err(Error::new(arg_error(), "Invalid validation type")),
                validation_type => Ok(ValidationRule {
                    column_name,
                    validation_type,
                }),
            }
        })
        .collect()
}

#[magnus::wrap(class = "CsvUtils::Validator")]
pub struct ValidatorWrapper {
    validator: RefCell<Validator>,
}

impl ValidatorWrapper {
    pub fn new_from_ruby(schema: RArray, error_log_path: String) -> Result<Self, Error> {
        let rules = ruby_rules_array_to_rules(schema)?;

        let validator = Validator::new(rules, error_log_path)
            .map_err(|e| Error::new(arg_error(), e.to_string()))?;
        Ok(Self {
            validator: RefCell::new(validator),
        })
    }

    pub fn validate_row(&self, row: Vec<String>) -> Result<bool, Error> {
        let result = self.validator.borrow_mut().validate_row(&row);
        Ok(result)
    }

    pub fn status(&self) -> Result<RHash, Error> {
        self.validator.borrow_mut().status()
    }
}

pub fn register(ruby: &Ruby, module: &RModule) -> Result<(), Error> {
    let class = module.define_class("Validator", ruby.class_object())?;
    class.define_singleton_method("new", function!(ValidatorWrapper::new_from_ruby, 2))?;
    class.define_method("validate_row", method!(ValidatorWrapper::validate_row, 1))?;
    class.define_method("status", method!(ValidatorWrapper::status, 0))?;
    Ok(())
}
