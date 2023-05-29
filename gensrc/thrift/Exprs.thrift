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
//
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Exprs.thrift

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp starrocks
namespace java com.starrocks.thrift

include "Types.thrift"
include "Opcodes.thrift"

enum TExprNodeType {
  // Be careful, to keep the compatibility between differen version fe and be,
  // please always add the new expr at last.
  AGG_EXPR,
  ARITHMETIC_EXPR,
  BINARY_PRED,
  BOOL_LITERAL,
  CASE_EXPR,
  CAST_EXPR,
  COMPOUND_PRED,
  DATE_LITERAL,
  FLOAT_LITERAL,
  INT_LITERAL,
  DECIMAL_LITERAL,
  IN_PRED,
  IS_NULL_PRED,
  LIKE_PRED,
  LITERAL_PRED,
  NULL_LITERAL,
  SLOT_REF,
  STRING_LITERAL,
  TUPLE_IS_NULL_PRED,
  INFO_FUNC,
  FUNCTION_CALL,

  // TODO: old style compute functions. this will be deprecated
  COMPUTE_FUNCTION_CALL,
  LARGE_INT_LITERAL,

  ARRAY_EXPR,
  ARRAY_ELEMENT_EXPR,
  ARRAY_SLICE_EXPR,

  TABLE_FUNCTION_EXPR,

  DICT_EXPR,
  PLACEHOLDER_EXPR,
  CLONE_EXPR,
  LAMBDA_FUNCTION_EXPR,
  SUBFIELD_EXPR,
  RUNTIME_FILTER_MIN_MAX_EXPR,
  MAP_ELEMENT_EXPR,
  BINARY_LITERAL,
  MAP_EXPR,
  DICT_QUERY_EXPR,
}

enum TIndexType {
  BITMAP
}

//enum TAggregationOp {
//  INVALID,
//  COUNT,
//  MAX,
//  DISTINCT_PC,
//  MERGE_PC,
//  DISTINCT_PCSA,
//  MERGE_PCSA,
//  MIN,
//  SUM,
//}
//
//struct TAggregateExpr {
//  1: required bool is_star
//  2: required bool is_distinct
//  3: required TAggregationOp op
//}
struct TAggregateExpr {
  // Indicates whether this expr is the merge() of an aggregation.
  1: required bool is_merge_agg
}
struct TBoolLiteral {
  1: required bool value
}

struct TCaseExpr {
  1: required bool has_case_expr
  2: required bool has_else_expr
}

struct TDateLiteral {
  1: required string value
}

struct TFloatLiteral {
  1: required double value
}

struct TDecimalLiteral {
  1: required string value
  2: optional binary integer_value
}

struct TIntLiteral {
  1: required i64 value
}

struct TLargeIntLiteral {
  1: required string value
}

struct TBinaryLiteral {
  1: required binary value
}

struct TInPredicate {
  1: required bool is_not_in
}

struct TIsNullPredicate {
  1: required bool is_not_null
}

struct TLikePredicate {
  1: required string escape_char;
}

struct TLiteralPredicate {
  1: required bool value
  2: required bool is_null
}

struct TTupleIsNullPredicate {
  1: required list<Types.TTupleId> tuple_ids
}

struct TSlotRef {
  1: required Types.TSlotId slot_id
  2: required Types.TTupleId tuple_id
}

struct TPlaceHolder {
  1: optional bool nullable;
  2: optional i32 slot_id;
}

struct TStringLiteral {
  1: required string value;
}

struct TInfoFunc {
  1: required i64 int_value;
  2: required string str_value;
}

struct TFunctionCallExpr {
  // The aggregate function to call.
  1: required Types.TFunction fn

  // If set, this aggregate function udf has varargs and this is the index for the
  // first variable argument.
  2: optional i32 vararg_start_idx
}

struct TSlotDescriptor {
  1: required Types.TSlotId id
  2: required Types.TTupleId parent
  3: required Types.TTypeDesc slotType
  4: required i32 columnPos   // in originating table
  5: required i32 byteOffset  // into tuple
  6: required i32 nullIndicatorByte
  7: required i32 nullIndicatorBit
  8: required string colName;
  9: required i32 slotIdx
  10: required bool isMaterialized
}

struct TTupleDescriptor {
  1: required Types.TTupleId id
  2: required i32 byteSize
  3: required i32 numNullBytes
  4: optional Types.TTableId tableId
  5: optional i32 numNullSlots
}

struct TOlapTableIndexTablets {
    1: required i64 index_id
    2: required list<i64> tablets
}

// its a closed-open range
struct TOlapTablePartition {
    1: required i64 id

    // how many tablets in one partition
    4: required i32 num_buckets

    5: required list<TOlapTableIndexTablets> indexes

    6: optional list<TExprNode> start_keys
    7: optional list<TExprNode> end_keys

    8: optional list<list<TExprNode>> in_keys
    // for automatic partition
    9: optional bool is_shadow_partition = false
}

struct TOlapTablePartitionParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version

    // used to split a logical table to multiple paritions
    // deprecated, use 'partition_columns' instead
    4: optional string partition_column

    // used to split a partition to multiple tablets
    5: optional list<string> distributed_columns

    // partitions
    6: required list<TOlapTablePartition> partitions

    7: optional list<string> partition_columns
    8: optional list<TExpr> partition_exprs

    9: optional bool enable_automatic_partition
}

struct TOlapTableIndexSchema {
    1: required i64 id
    2: required list<string> columns
    3: required i32 schema_hash
}

struct TOlapTableSchemaParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version

    // Logical columns, contain all column that in logical table
    4: required list<TSlotDescriptor> slot_descs
    5: required TTupleDescriptor tuple_desc
    6: required list<TOlapTableIndexSchema> indexes
}

struct TOlapTableIndex {
  1: optional string index_name
  2: optional list<string> columns
  3: optional TIndexType index_type
  4: optional string comment
}

struct TTabletLocation {
    1: required i64 tablet_id
    2: required list<i64> node_ids
}

struct TOlapTableLocationParam {
    1: required i64 db_id
    2: required i64 table_id
    3: required i64 version
    4: required list<TTabletLocation> tablets
}

struct TNodeInfo {
    1: required i64 id
    2: required i64 option
    3: required string host
    // used to transfer data between nodes
    4: required i32 async_internal_port
}

struct TNodesInfo {
    1: required i64 version
    2: required list<TNodeInfo> nodes
}

struct TDictQueryExpr {
  1: required TOlapTableSchemaParam schema
  2: required TOlapTablePartitionParam partition
  3: required TOlapTableLocationParam location
  4: required TNodesInfo nodes_info
  5: required map<i64, i64> partition_version
  6: list<string> key_fields
  7: string value_field
}

// This is essentially a union over the subclasses of Expr.
struct TExprNode {
  1: required TExprNodeType node_type
  2: required Types.TTypeDesc type
  3: optional Opcodes.TExprOpcode opcode
  4: required i32 num_children

  5: optional TAggregateExpr agg_expr
  6: optional TBoolLiteral bool_literal
  7: optional TCaseExpr case_expr
  8: optional TDateLiteral date_literal
  9: optional TFloatLiteral float_literal
  10: optional TIntLiteral int_literal
  11: optional TInPredicate in_predicate
  12: optional TIsNullPredicate is_null_pred
  13: optional TLikePredicate like_pred
  14: optional TLiteralPredicate literal_pred
  15: optional TSlotRef slot_ref
  16: optional TStringLiteral string_literal
  17: optional TTupleIsNullPredicate tuple_is_null_pred
  18: optional TInfoFunc info_func
  19: optional TDecimalLiteral decimal_literal

  20: required i32 output_scale
  21: optional TFunctionCallExpr fn_call_expr
  22: optional TLargeIntLiteral large_int_literal

  23: optional i32 output_column
  24: optional Types.TColumnType output_type
  25: optional Opcodes.TExprOpcode vector_opcode
  // The function to execute. Not set for SlotRefs and Literals.
  26: optional Types.TFunction fn
  // If set, child[vararg_start_idx] is the first vararg child.
  27: optional i32 vararg_start_idx
  28: optional Types.TPrimitiveType child_type

  29: optional TPlaceHolder vslot_ref;

  // Used for SubfieldExpr
  30: optional list<string> used_subfield_names;
  31: optional TBinaryLiteral binary_literal;

  // For vector query engine
  50: optional bool use_vectorized
  51: optional bool has_nullable_child
  52: optional bool is_nullable
  53: optional Types.TTypeDesc child_type_desc
  54: optional bool is_monotonic

  55: optional TDictQueryExpr dict_query_expr
}

struct TPartitionLiteral {
  1: optional Types.TPrimitiveType type
  2: optional TIntLiteral int_literal
  3: optional TDateLiteral date_literal
  4: optional TStringLiteral string_literal
}

// A flattened representation of a tree of Expr nodes, obtained by depth-first
// traversal.
struct TExpr {
  1: required list<TExprNode> nodes
}
