#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
                           bool only_full_group_by)
    : BaseSelectBinder(binder, context, node, info), only_full_group_by(only_full_group_by) {
}

unique_ptr<ParsedExpression> SelectBinder::GetSQLValueFunction(const string &column_name) {
	auto alias_entry = node.bind_state.alias_map.find(column_name);
	if (alias_entry != node.bind_state.alias_map.end()) {
		// don't replace SQL value functions if they are in the alias map
		return nullptr;
	}
	return ExpressionBinder::GetSQLValueFunction(column_name);
}

BindResult SelectBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	// first try to bind the column reference regularly
	auto expr_copy = expr_ptr->Copy();
	auto result = BaseSelectBinder::BindColumnRef(expr_ptr, depth, root_expression);
	if (!only_full_group_by && HasBoundColumns()) {
		vector<unique_ptr<ParsedExpression>> func_children;
		func_children.push_back(std::move(expr_copy));
		expr_copy = std::move(make_uniq<FunctionExpression>("any_value", std::move(func_children)));
		result = ExpressionBinder::BindExpression(expr_copy, depth);
		bound_columns.pop_back();
	}
	if (!result.HasError()) {
		return result;
	}
	// binding failed
	// check in the alias map
	auto &colref = (expr_ptr.get())->Cast<ColumnRefExpression>();
	if (!colref.IsQualified()) {
		auto &bind_state = node.bind_state;
		auto alias_entry = node.bind_state.alias_map.find(colref.column_names[0]);
		if (alias_entry != node.bind_state.alias_map.end()) {
			// found entry!
			auto index = alias_entry->second;
			if (index >= node.bound_column_count) {
				throw BinderException("Column \"%s\" referenced that exists in the SELECT clause - but this column "
				                      "cannot be referenced before it is defined",
				                      colref.column_names[0]);
			}
			if (bind_state.AliasHasSubquery(index)) {
				throw BinderException("Alias \"%s\" referenced in a SELECT clause - but the expression has a subquery."
				                      " This is not yet supported.",
				                      colref.column_names[0]);
			}
			auto copied_expression = node.bind_state.BindAlias(index);
			result = BindExpression(copied_expression, depth, false);
			return result;
		}
	}
	// entry was not found in the alias map: return the original error
	return result;
}

bool SelectBinder::QualifyColumnAlias(const ColumnRefExpression &colref) {
	if (!colref.IsQualified()) {
		return node.bind_state.alias_map.find(colref.column_names[0]) != node.bind_state.alias_map.end();
	}
	return false;
}

} // namespace duckdb
