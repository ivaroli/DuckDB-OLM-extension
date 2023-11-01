#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/execution/operator/persistent/parallel_csv_reader.hpp"
#include "duckdb/execution/operator/persistent/csv_file_handle.hpp"
#include "duckdb/execution/operator/persistent/csv_buffer.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

class OMLReader {
public:
	OMLReader(duckdb::vector<duckdb::LogicalType, true> return_types, duckdb::vector<std::string, true> names, duckdb::Value file):
	file(file.ToString()), return_types(return_types), names(names){}

	std::string file;
	vector<LogicalType> return_types;
	std::vector<std::string> names;

	std::vector<Value> ReadRow(idx_t index);

private:
	std::vector<std::string> SplitLine(std::istream& str);
};

}