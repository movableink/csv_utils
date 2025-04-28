# frozen_string_literal: true

require "csv_utils"
require "tempfile"
require "csv"

RSpec.describe CsvUtils::Generator do
  let(:input_data) {
    [
      "id,name,email,timestamp",
      "1,John Doe,john@example.com,2023-01-01",
      "2,Jane Smith,jane@example.com,2023-01-02",
      "1,John Updated,john@example.com,2023-01-03",  # Same id as first row, newer timestamp
      "3,Alice Johnson,alice@example.com,2023-01-04",
      "2,Jane Updated,jane@example.com,2023-01-05"   # Same id as second row, newer timestamp
    ].join("\n")
  }

  let(:input_path) {
    file = Tempfile.new(["input", ".csv"])
    file.write(input_data)
    file.close
    
    file.path
  }

  let(:output_path) {
    file = Tempfile.new(["output", ".csv"])
    file.close
    file.path
  }

  let(:rows) {
    CSV.read(output_path, headers: true)
  }

  after do
    File.unlink(input_path) if File.exist?(input_path)
    File.unlink(output_path) if File.exist?(output_path)
  end

  describe "CSV deduplication" do
    it "deduplicates records with the same key" do
      # Run deduplication using the first column (id) as the key
      result = CsvUtils::Generator.dedupe_csv(input_path, output_path, [0], max_records_per_key: 1)

      # Should have 3 rows (one per unique id), with the latest versions
      expect(rows.size).to eq(3)
      expect(rows.map { |r| r["id"] }.sort).to eq(["1", "2", "3"])
      
      # Verify we have the latest version of each record
      expect(rows.find { |r| r["id"] == "1" }["name"]).to eq("John Updated")
      expect(rows.find { |r| r["id"] == "2" }["name"]).to eq("Jane Updated")
      
      # Check the stats from the result
      expect(result[:records_written]).to eq(3)
    end
    
    describe "handles compound keys" do
      let(:input_data) {
        [
          "region,product,sales,timestamp",
          "east,widget,100,2023-01-01",
          "west,widget,150,2023-01-01",
          "east,gadget,200,2023-01-01",
          "west,gadget,250,2023-01-01",
          "east,widget,120,2023-01-02",  # Same region+product, newer timestamp
          "west,widget,170,2023-01-02"   # Same region+product, newer timestamp
        ].join("\n")
      }

      it "sorts properly" do        
        # Run deduplication using the first two columns (region, product) as compound key
        result = CsvUtils::Generator.dedupe_csv(input_path, output_path, [0, 1], max_records_per_key: 1)
        
        # Should have 4 unique combinations of region+product
        expect(rows.size).to eq(4)
        
        # Verify we have the latest version of each record
        east_widget = rows.find { |r| r["region"] == "east" && r["product"] == "widget" }
        expect(east_widget["sales"]).to eq("120") # newer value
        
        west_widget = rows.find { |r| r["region"] == "west" && r["product"] == "widget" }
        expect(west_widget["sales"]).to eq("170") # newer value
      end
    end
    
    describe "lots of duplicates" do
      let(:input_data) {
        data = ["id,value,timestamp"]
        300.times do |i|
          data << "same-key,value-#{i},2023-01-#{format('%02d', i+1)}"
        end
        data.join("\n")
      }

      it "limits to 200 records per key" do
        # Run deduplication
        result = CsvUtils::Generator.dedupe_csv(input_path, output_path, [0])
        
        # Should have exactly 200 records (the limit)
        expect(rows.size).to eq(200)
        
        # The records should be the 200 newest ones (highest timestamps)
        timestamps = rows.map { |r| r["timestamp"] }.sort
        expect(timestamps.first).to eq("2023-01-101") # 300 - 200 + 1 = 101
        expect(timestamps.last).to eq("2023-01-300")
        
        # Check the stats
        expect(result[:records_written]).to eq(200)
      end
    end
    
    describe "empty content" do
      let(:input_data) {
        "id,name,email\n"
      }

      it "returns an empty file" do
        # Run deduplication
        result = CsvUtils::Generator.dedupe_csv(input_path, output_path, [0])
        
        # Should have 0 data rows
        expect(rows.size).to eq(0)
      end
    end

    describe "empty file" do
      let(:input_data) {
        ""
      }

      it "throws an error" do
        # Run deduplication
        expect {
          CsvUtils::Generator.dedupe_csv(input_path, output_path, [0])
        }.to raise_error("No headers found in input file")
      end
    end
    
    describe "very large files" do
      let(:input_data) {
        data = ["id,value"]
        50_000.times do |i|
          data << "key-#{i % 1_000},value-#{i}"
        end
        data.join("\n")
      }

      it "handles large files by processing in chunks" do
        # Run deduplication
        result = CsvUtils::Generator.dedupe_csv(input_path, output_path, [0], buffer_size: 10_000)
        expect(result[:records_written]).to eq(50_000)
        expect(result[:run_files]).to eq(5)
        
        # Read the output file
        unique_keys = Set.new(rows.map { |r| r["id"] })
        
        # Should have 1000 unique keys
        expect(unique_keys.size).to eq(1_000)
      end
    end
  end
end 