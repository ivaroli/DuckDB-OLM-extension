#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "oml_functions.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
    // register table functions
    for (auto &fun : OMLFunctions::GetScalarFunctions()) {
      ExtensionUtil::RegisterFunction(instance, fun);
    }

    // register table functions
    for (auto &fun : OMLFunctions::GetTableFunctions()) {
      ExtensionUtil::RegisterFunction(instance, fun);
    }
}

void OmlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string OmlExtension::Name() {
	return "oml";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void oml_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *oml_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
