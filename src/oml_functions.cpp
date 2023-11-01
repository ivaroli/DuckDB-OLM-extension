#include "oml_functions.hpp"
#include "oml_reader.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/multi_file_reader.hpp"

namespace duckdb {

struct OMLReadGlobalState : public GlobalTableFunctionState {
public:
	idx_t chunk_count;
	idx_t rows_read;
	vector<LogicalType> types;
	OMLReader reader_bind;
};

struct OMLReadLocalState : public LocalTableFunctionState {
	// local state
};

struct OMLReadBindState : public TableFunctionData {
public:
	std::string file;
	vector<LogicalType> return_types;
	vector<std::string> names;
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
	auto &local_data = data_p.local_state->Cast<OMLReadGlobalState>();
	auto &bind_data = data_p.bind_data->Cast<OMLReadBindState>();
	idx_t rows_read_local = 0;

	do{
		std::vector<duckdb::Value> duck_row = global_data.reader_bind.ReadRow(global_data.rows_read);
		if (duck_row.empty()){break;}

		for (long unsigned int i = 0; i < duck_row.size(); i++) {
			auto v = duck_row.at(i);
			output.SetValue(i, output.size(), v);
			auto a = output.size();
		}

		global_data.rows_read++;
		rows_read_local++;
	} while(rows_read_local <= STANDARD_VECTOR_SIZE);

	output.SetCardinality(output.size()); // remember to test this
	global_data.chunk_count += 1;
}

unique_ptr<GlobalTableFunctionState> OMLFunctions::OMLScanInitGlobal(ClientContext &context, TableFunctionInitInput &input){
	// set the global state
	auto result = make_uniq<OMLReadGlobalState>();
	auto bind_data = input.bind_data->Cast<OMLReadBindState>();

	// create a reader
	result->reader_bind = OMLReader(bind_data.return_types, bind_data.names, bind_data.file);
	
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> OMLFunctions::OMLScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate_p){
	// set the local state
	auto result = make_uniq<OMLReadLocalState>();
	return std::move(result);
}

unique_ptr<FunctionData> OMLFunctions::OMLScanBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names){
	// this is called first - binding the scan to the OML scan function
	
	// hard coding the column names and types as they will always stay the same
	names.push_back("test");
	// names.push_back("experiment_id");
	// names.push_back("node_id");
	// names.push_back("node_id_seq");
	// names.push_back("time_sec");
	// names.push_back("time_usec");
	// names.push_back("power");
	// names.push_back("current");
	// names.push_back("voltage");

	return_types.push_back(LogicalType::BOOLEAN);
	// return_types.push_back(LogicalType::VARCHAR);
	// return_types.push_back(LogicalType::VARCHAR);
	// return_types.push_back(LogicalType::VARCHAR);
	// return_types.push_back(LogicalType::VARCHAR);
	// return_types.push_back(LogicalType::VARCHAR);
	// return_types.push_back(LogicalType::DOUBLE);
	// return_types.push_back(LogicalType::DOUBLE);
	// return_types.push_back(LogicalType::DOUBLE);

	auto file = input.inputs[0];
	auto result = make_uniq<OMLReadBindState>();
	
	result->file = file.ToString();
	result->names = names;
	result->return_types = return_types;

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