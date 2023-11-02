#include "oml_functions.hpp"
#include "oml_reader.hpp"

#include "duckdb.hpp"

namespace duckdb {

struct OMLReadGlobalState : public GlobalTableFunctionState {
public:
	idx_t chunk_count;
	idx_t rows_read;
	vector<LogicalType> types;
};

struct OMLReadLocalState : public LocalTableFunctionState {
	// local state
};

struct OMLReadBindState : public TableFunctionData {
public:
	std::string file;
	shared_ptr<OMLReader> reader_bind;
};

/////////////////////////////
/// Setup table functions ///
/////////////////////////////

vector<TableFunction> OMLFunctions::GetTableFunctions() {
	vector<TableFunction> functions;

    functions.push_back(OMLFunctions::GetOMLScanFunction());

	return functions;
}

TableFunction OMLFunctions::GetOMLScanFunction() {
	TableFunction table_function("oml_read", {LogicalType::VARCHAR}, OMLScanImplementation, OMLScanBind, OMLScanInitGlobal, OMLScanInitLocal);

	return table_function;
}

//////////////////////////////////////
/// Table functions implementation ///
//////////////////////////////////////

inline void OMLFunctions::OMLScanImplementation(ClientContext &context, TableFunctionInput &data_p, DataChunk &output){
	// this is the last call
	auto &global_data = data_p.global_state->Cast<OMLReadGlobalState>();
	auto &local_data = data_p.local_state->Cast<OMLReadLocalState>();
	auto &bind_data = data_p.bind_data->Cast<OMLReadBindState>();
	idx_t rows_read_local = 0;

	std::vector<duckdb::Value> duck_row{};

	while(rows_read_local < STANDARD_VECTOR_SIZE && bind_data.reader_bind->ReadRow(global_data.rows_read, duck_row)){
		if (duck_row.empty()){break;}

		for (long unsigned int i = 0; i < duck_row.size(); i++) {
			auto v = duck_row.at(i);
			output.SetValue(i, rows_read_local, v);
		}

		global_data.rows_read++;
		rows_read_local++;
		duck_row = std::vector<duckdb::Value>();
	}

	if (rows_read_local == 0)
	{
		return;
	}
	
	output.SetCardinality(rows_read_local); // remember to test this
	global_data.chunk_count += 1;
}

unique_ptr<GlobalTableFunctionState> OMLFunctions::OMLScanInitGlobal(ClientContext &context, TableFunctionInitInput &input){
	// set the global state
	auto result = make_uniq<OMLReadGlobalState>();
	auto bind_data = input.bind_data->Cast<OMLReadBindState>();

	// create a reader
	//result->reader_bind = OMLReader(bind_data.return_types, bind_data.names, bind_data.file);
	result->rows_read = 0;
	
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> OMLFunctions::OMLScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p){
	// set the local state
	auto result = make_uniq<OMLReadLocalState>();
	return std::move(result);
}

unique_ptr<FunctionData> OMLFunctions::OMLScanBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names){
	// this is called first - binding the scan to the OML scan function
	auto file = input.inputs[0];
	auto result = make_uniq<OMLReadBindState>();

	result->reader_bind = make_shared<OMLReader>(&return_types, &names, file);
	result->file = file.ToString();

	return std::move(result);
}

//////////////////////////////
/// Setup scalar functions ///
//////////////////////////////

vector<ScalarFunction> OMLFunctions::GetScalarFunctions() {
	vector<ScalarFunction> functions;

    functions.push_back(OMLFunctions::OMLScalarFun());

	return functions;
}

///////////////////////////////////////
/// Scalar functions implementation ///
///////////////////////////////////////

ScalarFunction OMLFunctions::OMLScalarFun(){
    return ScalarFunction("oml", {LogicalType::VARCHAR}, LogicalType::VARCHAR, OMLScalarFunInternal);
}

inline void OMLFunctions::OMLScalarFunInternal(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "OML "+name.GetString()+" 🐥");;
        });
}

}