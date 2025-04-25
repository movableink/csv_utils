# frozen_string_literal: true

require_relative "csv_validator/version"
require_relative "csv_validator/csv_validator"
require "set"

module CsvValidator
  class Error < StandardError; end
  
  # Result class to hold validation results
  class ValidationResult
    attr_reader :row_count, :error_summary, :error_logs_path, :reversed_output_path
    
    def initialize(row_count = 0, errors = {}, error_logs_path = nil, reversed_output_path = nil)
      @row_count = row_count.is_a?(Hash) ? row_count[:row_count] : row_count
      @error_summary = errors
      @error_logs_path = error_logs_path
      @reversed_output_path = reversed_output_path
    end
    
    def valid?
      @error_summary.empty?
    end

    def error_count
      @error_summary.values.sum
    end
  end

  class Validator
    attr_reader :reversed_output_path
    
    def initialize(pattern, error_logs_path: nil, reversed_output_path: nil)
      @pattern = pattern
      @error_logs_path = error_logs_path
      @reversed_output_path = reversed_output_path
      @result = nil
    end

    def validate_rows(file_path)
      @result = CsvValidator._validate(file_path, @pattern, @error_logs_path, @reversed_output_path)
      @result
    end

    def error_summary
      raise CsvValidator::Error, "No result" unless @result
      @result[:errors]
    end

    def error_count
      raise CsvValidator::Error, "No result" unless @result
      @result[:errors].values.sum
    end

    def valid?
      raise CsvValidator::Error, "No result" unless @result
      @result[:errors].empty?
    end

    def row_count
      raise CsvValidator::Error, "No result" unless @result
      @result[:row_count]
    end

    def validate_headers(current:, incoming:)
      current_headers = Set.new(current)
      incoming_headers = Set.new(incoming)

      valid = current_headers.subset?(incoming_headers)

      valid ? [] : generate_header_logs(current:, incoming:)
    end

    def generate_header_logs(current:, incoming:)
      logs = ["Column,Expected Header,Actual Header\n"]
      length = [current.length, incoming.length].max
      length.times do |index|
        logs << "#{index + 1},#{current[index]},#{incoming[index]}\n"
      end
      logs
    end
  end
  
  # Class method to validate CSV files
  # @param file_path [String] Path to the CSV file to validate
  # @param pattern [Array] Array of validation rules
  # @param error_log_path [String, nil] Optional path to write error log
  # @param reversed_output_path [String, nil] Optional path to write reversed CSV file
  # @return [ValidationResult] The validation result
  def self.validate(file_path, pattern, error_log_path = nil, reversed_output_path = nil)
    validator = Validator.new(pattern, error_logs_path: error_log_path, reversed_output_path: reversed_output_path)
    result = validator.validate_rows(file_path)
    ValidationResult.new(result[:row_count], result[:errors], error_log_path, reversed_output_path)
  end
end
