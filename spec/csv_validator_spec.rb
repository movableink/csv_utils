# frozen_string_literal: true

require "csv_validator"
require "tempfile"
require "set"

RSpec.describe CsvValidator do
  it "has a version number" do
    expect(CsvValidator::VERSION).not_to be nil
  end

  describe "URL validation" do
    it "validates valid URLs" do
      # Create a temp CSV file with valid URLs
      temp_csv = Tempfile.new(["valid_urls", ".csv"])
      temp_csv.write([
        "url",
        "https://example.com",
        "https://test.com/path?query=string#fragment"
      ].join("\n"))
      temp_csv.close
      
      # Create a pattern with URL validation for the first column
      pattern = [:url]
      
      validator = CsvValidator::Validator.new(pattern)
      validator.validate_rows(temp_csv.path)
      
      # Expect no errors
      expect(validator.valid?).to eq(true)
      expect(validator.error_summary).to eq({})
      
      # Clean up
      temp_csv.unlink
    end
    
    it "catches invalid URLs" do
      # Create a temp CSV file with an invalid URL
      temp_csv = Tempfile.new(["invalid_urls", ".csv"])
      temp_csv.write([
        "url",
        "https://example.com",
        "invalid-url-no-protocol"
      ].join("\n"))
      temp_csv.close
      
      # Create a pattern with URL validation for the first column
      pattern = [:url]
      
      # Expect validation to fail
      validator = CsvValidator::Validator.new(pattern)
      validator.validate_rows(temp_csv.path)
      
      expect(validator.valid?).to eq(false)
      expect(validator.error_summary).to eq({ url: 1 })
      
      # Clean up
      temp_csv.unlink
    end
  end

  describe "Protocol validation" do
    it "validates that values have a protocol" do
      # Create a temp CSV file with values having a protocol
      temp_csv = Tempfile.new(["protocol_values", ".csv"])
      temp_csv.write([
        "value",
        "http://example.com",
        "ftp://example.com",
        "sms://1234567890"
      ].join("\n"))
      temp_csv.close
      
      # Create a pattern with protocol validation for the first column
      pattern = [:protocol]
      
      # Expect no errors
      validator = CsvValidator::Validator.new(pattern)
      validator.validate_rows(temp_csv.path)
      
      expect(validator.valid?).to eq(true)
      expect(validator.error_summary).to eq({})
      
      # Clean up
      temp_csv.unlink
    end

    it "catches values without a protocol" do
      # Create a temp CSV file with values without a protocol
      temp_csv = Tempfile.new(["no_protocol", ".csv"])
      temp_csv.write([
        "value",
        "example.com",
        "1234567890"
      ].join("\n"))
      temp_csv.close
      
      # Create a pattern with protocol validation for the first column
      pattern = [:protocol]
      
      # Expect validation to fail
      validator = CsvValidator::Validator.new(pattern)
      validator.validate_rows(temp_csv.path)
      
      expect(validator.valid?).to eq(false)
      expect(validator.error_summary).to eq({ protocol: 2 })
      expect(validator.error_count).to eq(2)
      
      # Clean up
      temp_csv.unlink
    end

    it "ignores nil fields" do
      # Create a temp CSV file with nil fields
      temp_csv = Tempfile.new(["nil_fields", ".csv"])
      temp_csv.write([
        "key,value", 
        "key1,https://example.com",
        "key2,",
        "key3,https://example.com"
      ].join("\n"))
      temp_csv.close
      
      # Create a pattern with nil validation for the first column
      pattern = [nil]
      
      # Expect no errors
      validator = CsvValidator::Validator.new(pattern)
      validator.validate_rows(temp_csv.path)
      
      expect(validator.valid?).to eq(true)
      expect(validator.error_summary).to eq({})
      
      # Clean up
      temp_csv.unlink
    end    
  end

  describe "Multi validation" do
    it "validates multiple columns" do
      # Create a temp CSV file with multiple columns
      temp_csv = Tempfile.new(["multi_column", ".csv"])
      temp_csv.write([
        "url,other_url,name,protocol",
        "https://example.com,http://foo.com,John Doe,http://example.com"
      ].join("\n"))
      temp_csv.close

      # Create a pattern with URL, name, and protocol validation
      pattern = [:url, :url, nil, :protocol]

      # Expect no errors
      validator = CsvValidator::Validator.new(pattern)
      validator.validate_rows(temp_csv.path)
      
      expect(validator.valid?).to eq(true)
      expect(validator.error_summary).to eq({})

      # Clean up
      temp_csv.unlink
    end

    it "returns multiple errors" do
      # Create a temp CSV file with multiple columns
      temp_csv = Tempfile.new(["multi_column", ".csv"])
      temp_csv.write([
        "url,other_url,name,protocol",
        "https://example_com,foo.com,John Doe,example.com"
      ].join("\n"))
      temp_csv.close

      # Create a pattern with URL, name, and protocol validation
      pattern = [:url, :url, nil, :protocol]

      # Expect multiple errors
      validator = CsvValidator::Validator.new(pattern)
      validator.validate_rows(temp_csv.path)
      
      expect(validator.valid?).to eq(false)
      expect(validator.error_summary).to eq({ url: 1, protocol: 1 })
      expect(validator.error_count).to eq(2)

      # Clean up
      temp_csv.unlink
    end
  end

  describe "Error logging" do
    it "logs errors to a file" do
      # Create a temp CSV file with errors
      temp_csv = Tempfile.new(["errors", ".csv"])
      temp_csv.write([
        "my_url,other_url,name,my_protocol",
        "example.com,http://foo.com,John Doe,example"
      ].join("\n"))
      temp_csv.close

      # Create a pattern with URL, name, and protocol validation
      pattern = [:url, :url, nil, :protocol]

      # Create a temp file for error log
      error_log = Tempfile.new(["errors", ".log"], encoding: 'utf-8')
      error_log.close

      # Validate with error logging
      validator = CsvValidator::Validator.new(pattern, error_logs_path: error_log.path)
      validator.validate_rows(temp_csv.path)
      
      expect(validator.valid?).to eq(false)
      expect(validator.error_summary).to eq({ url: 1, protocol: 1 })

      # Check the error log
      error_log.open

      error_data = error_log.read.chomp

      expect(error_data).to eq([
        "Error Message,Row,Column",
        "example.com does not include a valid domain,1,my_url",
        "example does not include a valid link protocol,1,my_protocol"
      ].join("\n"))

      # Clean up
      temp_csv.unlink
      error_log.unlink
    end
  end
  
  describe "Instance methods" do
    it "validates rows using instance method" do
      temp_csv = Tempfile.new(["instance_test", ".csv"])
      temp_csv.write([
        "url,name",
        "https://example.com,John",
        "not-a-url,Jane"
      ].join("\n"))
      temp_csv.close
      
      # Create validator instance
      validator = CsvValidator::Validator.new([:url, nil])
      
      # Validate rows
      validator.validate_rows(temp_csv.path)
      expect(validator.valid?).to eq(false)
      expect(validator.error_summary).to eq({ url: 1 })
      
      temp_csv.unlink
    end
    
    it "validates headers" do
      # Test header validation
      current_headers = ["id", "name", "email"]
      incoming_headers = ["id", "name", "email", "extra"]
      
      validator = CsvValidator::Validator.new([])
      logs = validator.validate_headers(current: current_headers, incoming: incoming_headers)
      
      # All required headers are present, so should return empty array
      expect(logs).to eq([])
      
      # Test with missing headers
      current_headers = ["id", "name", "email"]
      incoming_headers = ["id", "name"]
      
      logs = validator.validate_headers(current: current_headers, incoming: incoming_headers)
      
      # Should return logs array with header information
      expect(logs.size).to be > 0
      expect(logs[0]).to eq("Column,Expected Header,Actual Header\n")
    end
  end
end
