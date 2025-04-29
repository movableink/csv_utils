require "csv_utils"
require "csv"

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
  it "should sort a CSV file" do
    sorter = CsvUtils::Sorter.new(source_id, [0], 100)
    sorter.add_row(["1", "2", "3"])
    sorter.add_row(["4", "5", "6"])
    result = sorter.sort!
    expect(result[:total_rows]).to eq(2)
    expect(collect_rows(sorter)).to eq([["1", "2", "3"], ["4", "5", "6"]])
  end

  it "sorts a CSV file with compound keys" do
    sorter = CsvUtils::Sorter.new(source_id, [0, 1], 100)
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
    sorter = CsvUtils::Sorter.new(source_id, [0, 1], 100)
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
    sorter = CsvUtils::Sorter.new(source_id, [0, 1], 100)
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
    sorter = CsvUtils::Sorter.new(source_id, [0], 100)
    sorter.set_validation_schema([:url])
    sorter.add_row(["https://example.com"])
    sorter.add_row(["test.com"])

    result = sorter.sort!
    expect(result[:failed_url_error_count]).to eq(1)    
    expect(result[:total_rows_processed]).to eq(2)
    expect(result[:total_rows]).to eq(1)
  end
end
