# frozen_string_literal: true

require "set"
require "fileutils"
require_relative "error"
require_relative "csv_validator"

module CsvValidator
  class Generator
    # Default values
    DEFAULT_MAX_RECORDS_PER_KEY = 200
    DEFAULT_BUFFER_SIZE_MB = 200
    
    # Deduplicate a CSV file based on key columns
    # @param input_path [String] Path to the input CSV file
    # @param output_path [String] Path to write the deduplicated output
    # @param key_columns [Array<Integer>] Array of column indices to use as compound key
    # @param options [Hash] Optional parameters
    # @option options [Integer] :max_records_per_key Maximum records to keep per unique key (default: 200)
    # @option options [Integer] :buffer_size_mb Size of each temporary file in MB (default: 200)
    # @return [Hash] Statistics about the deduplication
    def self.dedupe_csv(input_path, output_path, key_columns, options = {})
      # Ensure input file exists
      raise "Input file not found: #{input_path}" unless File.exist?(input_path)
      
      # Ensure output directory exists
      output_dir = File.dirname(output_path)
      FileUtils.mkdir_p(output_dir) unless File.directory?(output_dir)
      
      # Convert paths to absolute paths to ensure Rust code can find them
      abs_input_path = File.expand_path(input_path)
      abs_output_path = File.expand_path(output_path)
      
      # Get optional parameters with defaults
      max_records = options[:max_records_per_key] || DEFAULT_MAX_RECORDS_PER_KEY
      buffer_size_mb = options[:buffer_size_mb] || DEFAULT_BUFFER_SIZE_MB
      
      # Call the native method
      begin
        CsvValidator._dedupe(abs_input_path, abs_output_path, key_columns, max_records, buffer_size_mb)
      rescue RuntimeError => e
        if e.message == "No records found in input file" && 
           File.size?(abs_input_path) <= 5 # Only header or empty file
          
          # Handle special case for empty files - just copy the header
          if File.exist?(abs_input_path)
            header = File.open(abs_input_path, &:readline)
            File.write(abs_output_path, header)
            return { records_written: 0, run_files: 0 }
          end
        end
        raise e
      end
    end
  end
end
