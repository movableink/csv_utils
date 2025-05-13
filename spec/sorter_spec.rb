require "csv_utils"
require "csv"
require "activerecord-copy"

LITTLE_ENDIAN_BYTE_ORDER = 0x01
POINT_TYPE_ID = 0x01 | 0x20000000  # Point type with SRID flag

def generate_binary_ewkb(longitude, latitude, srid)
  [
    LITTLE_ENDIAN_BYTE_ORDER,
    *[POINT_TYPE_ID].pack('L<').bytes,  # Type ID with SRID flag
    *[srid].pack('L<').bytes,
    *[latitude].pack('E').bytes,
    *[longitude].pack('E').bytes
  ].pack('C*').force_encoding('ASCII-8BIT')
end

def collect_rows(sorter)
  rows = []
  sorter.each_batch(1) do |batch|
    batch.each do |item|
      rows << item[1]
    end
  end
  rows
end

RSpec.describe CsvUtils::Sorter do
  let(:source_id) { "1" }
  let(:source_key) { "12345abcdef" }
  let(:error_log_path) { Tempfile.new.path }

  it "should sort a CSV file" do
    sorter = CsvUtils::Sorter.new(source_id, source_key, [0], nil, 100)
    sorter.add_row(["1", "2", "3"])
    sorter.add_row(["4", "5", "6"])
    result = sorter.sort!
    expect(result[:total_rows]).to eq(2)
    expect(collect_rows(sorter)).to eq([["1", "2", "3"], ["4", "5", "6"]])
  end

  it "sorts a CSV file with compound keys" do
    sorter = CsvUtils::Sorter.new(source_id, source_key, [0, 1], nil, 100)
    sorter.add_row(["1", "2", "3"])
    sorter.add_row(["1", "3", "2"])
    sorter.add_row(["3", "1", "3"])
    sorter.add_row(["2", "3", "1"])
    sorter.add_row(["3", "1", "2"])
    sorter.add_row(["3", "2", "2"])
    sorter.add_row(["3", "2", "1"])
    result = sorter.sort!
    expect(result[:total_rows]).to eq(7)
    expect(collect_rows(sorter)).to eq(
      [
        ["1", "2", "3"], 
        ["3", "2", "2"], 
        ["3", "2", "1"], 
        ["2", "3", "1"], 
        ["1", "3", "2"], 
        ["3", "1", "3"], 
        ["3", "1", "2"]
      ]
    )
  end

  it "yields batches of sorted rows" do
    sorter = CsvUtils::Sorter.new(source_id, source_key, [0, 1], nil, 100)
    sorter.add_row(["1", "2", "3"])
    sorter.add_row(["4", "5", "6"])
    result = sorter.sort!
    expect(result[:total_rows]).to eq(2)
    count = 0
    sorter.each_batch(1) do |batch|
      expect(batch.size).to eq(1)
      count += 1
    end
    expect(count).to eq(2)
  end

  it "yields multiple results in a batch" do
    sorter = CsvUtils::Sorter.new(source_id, source_key, [0, 1], nil, 100)
    sorter.add_row(["1", "2", "3"])
    sorter.add_row(["1", "3", "2"])
    sorter.add_row(["3", "1", "3"])
    sorter.add_row(["2", "3", "1"])

    result = sorter.sort!
    expect(result[:total_rows]).to eq(4)
    count = 0
    sorter.each_batch(10) do |batch|
      expect(batch).to eq([
        ["6ea87ee6f25f25d1e14c442a890eda7c722bca7a", ["1", "2", "3"]], 
        ["b85e2d4914e22b5ad3b82b312b3dc405dc17dcb8", ["2", "3", "1"]], 
        ["3c9db9ba838cbefabdbd7ce6c6ca549d3f0e6743", ["1", "3", "2"]],
        ["0d1a3778431c4f1daffc613e793225ca2fee71c4", ["3", "1", "3"]]
      ])
      count += 1
    end
    expect(count).to eq(1)
  end

  it "validates on add_row" do
    sorter = CsvUtils::Sorter.new(source_id, source_key, [0], nil, 100)
    sorter.enable_validation([{column_name: "my_url", validation_type: :url}], error_log_path)
    sorter.add_row(["https://example.com"])
    sorter.add_row(["test.com"])

    result = sorter.sort!
    expect(result[:failed_url_error_count]).to eq(1)    
    expect(result[:total_rows_processed]).to eq(2)
    expect(result[:total_rows]).to eq(1)

    expect(File.read(error_log_path)).to include("my_url does not include a valid domain,2,1")
  end

  describe "writing a binary postgres file" do
    let(:outfile_path) { Tempfile.new.path }

    it "writes a binary postgres file" do
      sorter = CsvUtils::Sorter.new(source_id, source_key, [0, 1], nil, 100)
      sorter.add_row(["1", "2", "3"])
      sorter.add_row(["4", "5", "6"])
      sorter.sort!
      sorter.write_binary_postgres_file(outfile_path)
      expect(File.exist?(outfile_path)).to be_truthy
      expect(File.size(outfile_path)).to be > 0
      
      decoder = ActiveRecordCopy::Decoder.new(file: outfile_path, column_types: [:text, :text, :bytea, :"character[]", :timestamp, :timestamp])
      results = []
      decoder.each { |result| results << result }
      expect(results).to match_array([
        [
          source_key,
          "d2736c67cf4728de554175f2533dc6662522db5b",
          nil,
          ["4", "5", "6"],
          anything,
          anything
        ],
       [
          source_key,
          "6ea87ee6f25f25d1e14c442a890eda7c722bca7a",
          nil,
          ["1", "2", "3"],
          anything,
          anything
        ]
      ])
    end

    it "writes a binary postgres file with geometry" do
      sorter = CsvUtils::Sorter.new(source_id, source_key, [0, 1], [2, 3], 100)
      sorter.add_row(["1", "hello", "-74.006", "40.7128"])
      sorter.add_row(["4", "world", "-71.006", "44.7128"])
      sorter.sort!
      sorter.write_binary_postgres_file(outfile_path)
      `cp #{outfile_path} /tmp/bincopy.bin`
      expect(File.exist?(outfile_path)).to be_truthy
      expect(File.size(outfile_path)).to be > 0
      
      decoder = ActiveRecordCopy::Decoder.new(file: outfile_path, column_types: [:text, :text, :bytea, :"character[]", :timestamp, :timestamp])
      results = []
      decoder.each { |result| results << result }

      expect(results).to match_array([
        [
          source_key,
          "81dda56703aa9978ce2bc1212c9d96b7ddcbf599",
          generate_binary_ewkb(-71.006, 44.7128, 4326),
          ["4", "world", "-71.006", "44.7128"],
          anything,
          anything
        ],
        [
          source_key,
          "7ff8c9efec43aadca084abbf7ef9da0d0b65fb84",
          generate_binary_ewkb(-74.006, 40.7128, 4326),
          ["1", "hello", "-74.006", "40.7128"],
          anything,
          anything
        ]
      ])
    end
  end
end