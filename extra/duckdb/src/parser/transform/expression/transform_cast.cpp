#include "duckdb/common/limits.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformTypeCast(duckdb_libpgquery::PGTypeCast &root) {
	// get the type to cast to
	auto type_name = root.typeName;
	LogicalType target_type = TransformTypeName(*type_name);

	// check for a constant BLOB value, then return ConstantExpression with BLOB
	if (!root.tryCast && target_type == LogicalType::BLOB && root.arg->type == duckdb_libpgquery::T_PGAConst) {
		auto c = PGPointerCast<duckdb_libpgquery::PGAConst>(root.arg);
		if (c->val.type == duckdb_libpgquery::T_PGString) {
			CastParameters parameters;
			if (root.location >= 0) {
				parameters.query_location = NumericCast<idx_t>(root.location);
			}
			auto blob_data = Blob::ToBlob(string(c->val.val.str), parameters);
			return make_uniq<ConstantExpression>(Value::BLOB_RAW(blob_data));
		}
	}

	// transform the expression node
	auto expression = TransformExpression(root.arg);
	bool try_cast = root.tryCast;

	// now create a cast operation
	auto result = make_uniq<CastExpression>(target_type, std::move(expression), try_cast);
	SetQueryLocation(*result, root.location);
	if (target_type == LogicalTypeId::VARCHAR) {
		if (type_name->typmods) {
			std::vector<unique_ptr<ParsedExpression>> children;
			auto &const_val = *Transformer::PGPointerCast<duckdb_libpgquery::PGAConst>(type_name->typmods->head->data.ptr_value);
			children.push_back(std::move(result));
			children.push_back(std::move(make_uniq<ConstantExpression>(Value::BIGINT(const_val.val.val.ival)) ));
			auto function = make_uniq<FunctionExpression>("left", std::move(children));
			return std::move(function);
		}
	}
	return std::move(result);
}

} // namespace duckdb
