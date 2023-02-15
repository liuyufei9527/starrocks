// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/dict_query_expr.h"

#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "gutil/casts.h"
#include "storage/chunk_helper.h"
#include "storage/table_reader.h"
#include "udf/java/utils.h"

namespace starrocks {


DictQueryExpr::DictQueryExpr(const TExprNode& node) : Expr(node), _dict_query_expr(node.dict_query_expr) {}

DictQueryExpr::~DictQueryExpr() {
    /*auto promise = call_function_in_pthread(_runtime_state, [this]() {
        // end
        return Status::OK();
    });
    promise->get_future().get();*/
}

StatusOr<ColumnPtr> DictQueryExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    Columns columns(children().size());
    size_t size = ptr != nullptr ? ptr->num_rows() : 1;
    for (int i = 0; i < _children.size(); ++i) {
        ASSIGN_OR_RETURN(columns[i], _children[i]->evaluate_checked(context, ptr));
    }

    //ColumnPtr res = ColumnHelper::create_column(TypeDescriptor::from_primtive_type(TYPE_BIGINT), true);
    ColumnPtr res;
    //res->resize(size);
    auto call_udf = [&]() {
        //context->fn_context(_fn_context_index), columns,

        /*for (auto& column : columns) {
            if (column->only_null()) {
                // we will handle NULL later
            } else if (column->is_constant()) {
                column = ColumnHelper::unpack_and_duplicate_const_column(size, column);
            }
        }*/
        //auto* nullable_column = down_cast<NullableColumn*>(res.get());
        //Column* dataColumn = nullable_column->mutable_data_column();


        ChunkPtr keyChunk = ChunkHelper::new_chunk(_key_schema, size);

        keyChunk->reset();
        ColumnPtr keyColumn = columns[1];
        /*if (keyColumn.is_nullable()) {

        } else {
            keyChunk->update_column_by_index(keyColumn, 0);
        }*/
        ColumnViewer<TYPE_VARCHAR> key_viewer(keyColumn);


        vector<int> res_index;
        res_index.resize(size);
        int not_null_size = 0;
        for (int idx = 0; idx < size; ++idx) {
            if (!key_viewer.is_null(idx)) {
                keyChunk->get_column_by_index(0)->append_datum(Datum(key_viewer.value(idx)));
                res_index[idx] = not_null_size;
                not_null_size++;
            }
        }
        //ColumnPtr keyColumn = keyChunk->get_column_by_index(0);

        //columns[1]->is_nullable()
        //keyChunk->update_column_by_index(columns[1], 0);
        /*for (int idx = 0; idx < size; ++idx) {
            Slice s = str_viewer.value(idx);
            keyColumn->append_datum(Datum(s));
        }*/
        //const std::shared_ptr<Column>& src = std::move();
        //keyChunk->append_column(columns[1], _key_schema.field(0));

        std::vector<bool> found;
        //found.resize(size);
        ChunkPtr valueChunk = ChunkHelper::new_chunk(_value_schema, keyChunk->num_rows());

        Status status = _table_reader->multi_get(*keyChunk, {_dict_query_expr.value_field}, found, *valueChunk);
        ColumnViewer<TYPE_BIGINT> value_viewer(valueChunk->get_column_by_index(0));
        res = _value_schema.field(0)->create_column();
        for (int idx = 0; idx < size; ++idx) {
            if (!key_viewer.is_null(idx) && found[res_index[idx]]) {
                res->append_datum(Datum(value_viewer.value(res_index[idx])));
            } else {
                res->append_nulls(1);
            }
        }

        //res = ColumnHelper::create_column()


        /*for (int idx = 0; idx < size; ++idx) {
            if (str_viewer.is_null(idx)) {

            }
            Slice s = str_viewer.value(idx);
            long v = value_viewer.value(idx);

            //res->append_datum(Datum(v));
            LOG(INFO) << "query dict, key: " << s.to_string() << ", table_id: "
                      << _dict_query_expr.table_id << ", found: " << found[idx]
                      << "res_id: " << v;
        }*/

        return Status::OK();
    };
    call_udf();
    //call_function_in_pthread(_runtime_state, call_udf)->get_future().get();
    return res;
}

Status DictQueryExpr::prepare(RuntimeState* state, ExprContext* context) {
    _runtime_state = state;
    // init Expr::prepare

    return Status::OK();
}

Status DictQueryExpr::open(RuntimeState* state, ExprContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    // init parent open
    RETURN_IF_ERROR(Expr::open(state, context, scope));

    TableReaderParams params;
    params.schema = _dict_query_expr.schema;
    params.partition_param = _dict_query_expr.partition;
    params.location_param = _dict_query_expr.location;
    params.nodes_info = _dict_query_expr.nodes_info;
    params.partition_versions = _dict_query_expr.partition_version;
    params.timeout_ms = 30000;

    _table_reader = std::make_shared<TableReader>();
    _table_reader->init(params);

    for (int i = 0; i < _dict_query_expr.key_fields.size(); ++i) {
        FieldPtr key_field = std::make_shared<Field>(i, _dict_query_expr.key_fields[i], TYPE_VARCHAR, false);
        _key_schema.append(key_field);
    }
    FieldPtr value_field = std::make_shared<Field>(0, _dict_query_expr.value_field, TYPE_BIGINT, false);
    _value_schema.append(value_field);

    return Status::OK();
}

void DictQueryExpr::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    Expr::close(state, context, scope);
}


} // namespace starrocks::vectorized
