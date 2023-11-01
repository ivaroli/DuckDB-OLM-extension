#pragma once

#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

class OMLFunctions {
public:
	static vector<TableFunction> GetTableFunctions();
	static vector<ScalarFunction> GetScalarFunctions();

private:
    // table functions definition
	static TableFunction GetOMLScanFunction();
	static TableFunction GetOMLMetadataFunction();

    // table functions internal
    static void OMLScanImplementation(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p, duckdb::DataChunk &output);
    static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> OMLScanInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input);
    static unique_ptr<FunctionData> OMLScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names);
    static duckdb::unique_ptr<duckdb::LocalTableFunctionState> OMLScanInitLocal(duckdb::ExecutionContext &context, 
                                                    duckdb::TableFunctionInitInput &input, duckdb::GlobalTableFunctionState *gstate_p);

    // scalar functions definitions
    static ScalarFunction OMLScalarFun();

    // scalar functions internal
    static inline void OMLScalarFunInternal(DataChunk &args, ExpressionState &state, Vector &result);
};

}