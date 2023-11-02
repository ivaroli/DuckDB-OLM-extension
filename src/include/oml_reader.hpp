#pragma once

#include <iostream>
#include <fstream>
#include <vector>
#include "duckdb.hpp"

namespace duckdb {

class OMLReader {
public:
	OMLReader();
	OMLReader(std::vector<duckdb::LogicalType>* return_types, std::vector<std::string>* names, duckdb::Value file);

	std::string file;
	std::vector<duckdb::LogicalType> *return_types;
	std::vector<std::string> *names;

	bool ReadRow(idx_t index, std::vector<Value> &row);

private:
	std::vector<std::string> lines;
	idx_t data_start;

	void ProcessMetadata(std::vector<duckdb::LogicalType> *return_types, std::vector<std::string> *names);
	void ProcessMetadataTypes(std::vector<std::string> parts, std::vector<duckdb::LogicalType> *return_types, std::vector<std::string> *names);
	std::vector<std::string> SplitLine(std::istream& str, char delim);
};

}