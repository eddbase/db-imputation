//
// Created by Massimo Perini on 29/05/2023.
//

/*
 * CUSTOM DATATYPE
namespace duckdb {

    struct StructPackFun {
        static constexpr const char *Name = "lift";
        static constexpr const char *Parameters = "any";
        static constexpr const char *Description = "Create a STRUCT containing the argument values. The entry name will be the bound variable name";
        static constexpr const char *Example = "struct_pack(i := 4, s := 'string')";

        static ScalarFunction GetFunction();
    };

    //copy datachunk to result
    static void StructPackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
#ifdef DEBUG
        auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<VariableReturnBindData>();
	// this should never happen if the binder below is sane
	D_ASSERT(args.ColumnCount() == StructType::GetChildTypes(info.stype).size());
#endif
        bool all_const = true;
        auto &child_entries = StructVector::GetEntries(result);
        for (size_t i = 0; i < args.ColumnCount(); i++) {
            if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
                all_const = false;
            }
            // same holds for this
            child_entries[i]->Reference(args.data[i]);
        }
        result.SetVectorType(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);

        result.Verify(args.size());
    }

    static unique_ptr<FunctionData> StructPackBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
        case_insensitive_set_t name_collision_set;

        // collect names and deconflict, construct return type
        if (arguments.empty()) {
            throw Exception("Can't pack nothing into a struct");
        }
        child_list_t<LogicalType> struct_children;
        for (idx_t i = 0; i < arguments.size(); i++) {
            auto &child = arguments[i];
            if (child->alias.empty() && bound_function.name == "struct_pack") {
                throw BinderException("Need named argument for struct pack, e.g. STRUCT_PACK(a := b)");
            }
            if (child->alias.empty() && bound_function.name == "row") {
                child->alias = "v" + std::to_string(i + 1);
            }
            if (name_collision_set.find(child->alias) != name_collision_set.end()) {
                throw BinderException("Duplicate struct entry name \"%s\"", child->alias);
            }
            name_collision_set.insert(child->alias);
            struct_children.push_back(make_pair(child->alias, arguments[i]->return_type));
        }

        // this is more for completeness reasons
        bound_function.return_type = LogicalType::STRUCT(struct_children);//given params, create a struct type
        return make_uniq<VariableReturnBindData>(bound_function.return_type);
    }

        /*unique_ptr<BaseStatistics> StructPackStats(ClientContext &context, FunctionStatisticsInput &input) {
        auto &child_stats = input.child_stats;
        auto &expr = input.expr;
        auto struct_stats = StructStats::CreateUnknown(expr.return_type);
        for (idx_t i = 0; i < child_stats.size(); i++) {
            StructStats::SetChildStats(struct_stats, i, child_stats[i]);
        }
        return struct_stats.ToUnique();
    }

    ScalarFunction StructPackFun::GetFunction() {
        // the arguments and return types are actually set in the binder function
        ScalarFunction fun("struct_pack", {}, LogicalTypeId::STRUCT, StructPackFunction, StructPackBind, nullptr,
                           StructPackStats);
        fun.varargs = LogicalType::ANY;
        fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
        fun.serialize = VariableReturnBindData::Serialize;
        fun.deserialize = VariableReturnBindData::Deserialize;
        return fun;

 */