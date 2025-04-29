use std::error::Error as StdError;
use std::fmt;
use url::Url;
use magnus::{prelude::*, Error, Ruby, RModule, method, function, exception::arg_error, RHash, RArray, Value, Symbol};
use std::cell::RefCell;
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

enum ValidationRule {
    Ignore,       // Ignore this column
    Url,          // Validate as URL
    Protocol,     // Check if it contains ://
}

pub struct Validator {
    rules: Vec<ValidationRule>,
    pub total_rows: usize,
    pub failed_url_error_count: usize,
    pub failed_protocol_error_count: usize,
}

fn parse_validation_pattern(pattern: &Vec<String>) -> Result<Vec<ValidationRule>, ValidationError> {
  let rules: Result<Vec<ValidationRule>, ValidationError> = pattern.iter().map(|p| {
    if p.is_empty() {
      Ok(ValidationRule::Ignore)
    } else if p == "url" {
      Ok(ValidationRule::Url)
    } else if p == "protocol" {
      Ok(ValidationRule::Protocol)
    } else {
      Err(ValidationError { message: format!("Unknown validation rule: {}", p) })
    }
  }).collect();
  
  rules
}

impl Validator {
    pub fn new(pattern: Vec<String>) -> Result<Self, ValidationError> {
        let rules = parse_validation_pattern(&pattern)?;
        Ok(Self { rules, total_rows: 0, failed_url_error_count: 0, failed_protocol_error_count: 0 })
    }

    pub fn validate_row(&mut self, row: &Vec<String>) -> bool {
        let mut failed_url = false;
        let mut failed_protocol = false;

        for (col_idx, rule) in self.rules.iter().enumerate() {
            let field = &row[col_idx];

            match rule {
                ValidationRule::Ignore => continue,
                ValidationRule::Url => {
                    if !field.is_empty() {
                        if let Err(_) = Url::parse(field) {
                            failed_url = true;
                        }
                    }
                }
                ValidationRule::Protocol => {
                    if !field.is_empty() && !field.contains("://") {
                        failed_protocol = true;
                    }
                }
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
}

#[magnus::wrap(class = "CsvUtils::Validator")]
pub struct ValidatorWrapper {
  validator: RefCell<Validator>,
}

impl ValidatorWrapper {
  pub fn new_from_ruby(schema: RArray) -> Result<Self, Error> {
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

    let validator = Validator::new(pattern).map_err(|e| Error::new(arg_error(), e.to_string()))?;
    Ok(Self { validator: RefCell::new(validator) })
  }

  pub fn validate_row(&self, row: Vec<String>) -> Result<bool, Error> {
    let result = self.validator.borrow_mut().validate_row(&row);
    Ok(result)
  }

  pub fn status(&self) -> Result<RHash, Error> {
    let validator = self.validator.borrow();
    let status = RHash::new();

    let _ = status.aset(Symbol::new("total_rows"), validator.total_rows);
    let _ = status.aset(Symbol::new("failed_url_error_count"), validator.failed_url_error_count);
    let _ = status.aset(Symbol::new("failed_protocol_error_count"), validator.failed_protocol_error_count);

    Ok(status)
  }
}

pub fn register(ruby: &Ruby, module: &RModule) -> Result<(), Error> {
  let class = module.define_class("Validator", ruby.class_object())?;
  class.define_singleton_method("new", function!(ValidatorWrapper::new_from_ruby, 1))?;
  class.define_method("validate_row", method!(ValidatorWrapper::validate_row, 1))?;
  class.define_method("status", method!(ValidatorWrapper::status, 0))?;
  Ok(())
}