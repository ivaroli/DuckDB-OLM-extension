#include "oml_reader.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/execution/operator/scan/csv/csv_line_info.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include <limits>

namespace duckdb {

std::vector<std::string> OMLReader::SplitLine(std::istream& str)
{
    std::vector<std::string> result;
    std::string line;
    std::getline(str,line);

    std::stringstream lineStream(line);
    std::string cell;

    while(std::getline(lineStream,cell, '\t'))
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