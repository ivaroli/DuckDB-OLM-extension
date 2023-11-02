#include "oml_reader.hpp"
#include "duckdb.hpp"
#include <vector>
#include <iostream>

#include <limits>

namespace duckdb {

OMLReader::OMLReader(){}

OMLReader::OMLReader(std::vector<LogicalType> *return_types, std::vector<std::string> *names, duckdb::Value file):
file(file.ToString()), names(names), return_types(return_types){
	std::ifstream infile(this->file);
	this->lines = std::vector<std::string>();

	for (std::string line; std::getline(infile, line); ) {
		this->lines.push_back(line);
	}

	this->ProcessMetadata(this->return_types, this->names);
	this->data_start = 9; // TODO: Fix
}

bool OMLReader::ReadRow(idx_t index, std::vector<Value> &row){
	if(this->data_start + index >= this->lines.size()){
		return false;
	}

	std::string line = this->lines.at(this->data_start + index);
	std::istringstream stream(line);
	
	auto words = this->SplitLine(stream, '\t');

	for (int i = 0; i < words.size(); i++){
		//auto type = this->return_types->at(i);
		duckdb::Value val(words.at(i));
		
		row.push_back(val);
	}

	return true;
}

void OMLReader::ProcessMetadata(std::vector<duckdb::LogicalType>* return_types, std::vector<std::string>* names){
	int maxSchemaNumber = -1, row = 0;

	while(true){
		auto current_row = std::istringstream(this->lines.at(row));
		auto words = this->SplitLine(current_row, ' ');

		if (maxSchemaNumber >= 0 && (words.empty() || words.at(0) != "schema:")){
			//this->data_start = row;
			break;
		} else if (words.at(0) == "schema:"){
			maxSchemaNumber++;
			for(idx_t i = 3; i < words.size(); i++){
				if(words.at(i).empty()){continue;}
				auto word = std::istringstream(words.at(i));
				auto parts = this->SplitLine(word, ':');
				this->ProcessMetadataTypes(parts, return_types, names);
			}
		}
		row++;
	}
}

void OMLReader::ProcessMetadataTypes(std::vector<std::string> parts, std::vector<duckdb::LogicalType>* return_types, std::vector<std::string>* names){
	names->push_back(parts.at(0));
	if(parts.at(1).find("int") != std::string::npos){
		return_types->push_back(duckdb::LogicalType::INTEGER);
	} else if(parts.at(1) == "string"){
		return_types->push_back(duckdb::LogicalType::VARCHAR);
	} else if(parts.at(1) == "double"){
		return_types->push_back(duckdb::LogicalType::DOUBLE);
	} else {
		return_types->push_back(duckdb::LogicalType::ANY);
	}
}

std::vector<std::string> OMLReader::SplitLine(std::istream& str, char delim){
    std::vector<std::string> result;
    std::string line;
    std::getline(str,line);

    std::stringstream lineStream(line);
    std::string cell;

    while(std::getline(lineStream,cell, delim))
    {
        result.push_back(cell);
    }

    if (!lineStream && cell.empty())
    {
        result.push_back("");
    }
    return result;
}

}