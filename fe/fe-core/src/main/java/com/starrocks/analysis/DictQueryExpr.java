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


package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.UserException;
import com.starrocks.planner.OlapTableParamGenerator;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TDictQueryExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TOlapTableSchemaParam;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.thrift.TTupleDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// TODO
public class DictQueryExpr extends FunctionCallExpr {

    private TDictQueryExpr dictQueryExpr;

    public DictQueryExpr(long dbId, OlapTable dictTable, Expr dictExpr, Expr keyExpr,
            StringLiteral keyFieldExpr, StringLiteral valueFieldExpr) throws SemanticException {
        super(FunctionSet.DICT_GET, Arrays.asList(dictExpr, keyExpr, keyFieldExpr, valueFieldExpr));

        this.dictQueryExpr = new TDictQueryExpr();
        TOlapTableSchemaParam schema = OlapTableParamGenerator.createSchema(dbId, dictTable);
        TupleDescriptor tupleDescriptor = new TupleDescriptor(TupleId.createGenerator().getNextId());
        IdGenerator<SlotId> slotIdIdGenerator = SlotId.createGenerator();

        for (Column column : dictTable.getBaseSchema()) {
            //SlotDescriptor slotDescriptor = new SlotDescriptor(slotIdIdGenerator.getNextId(), column.getName(), column.getType(), column.isAllowNull());
            SlotDescriptor slotDescriptor = new SlotDescriptor(slotIdIdGenerator.getNextId(), tupleDescriptor);
            slotDescriptor.setColumn(column);
            tupleDescriptor.addSlot(slotDescriptor);
        }
        for (SlotDescriptor slot : tupleDescriptor.getSlots()) {
            schema.addToSlot_descs(slot.toThrift());
        }
        schema.setTuple_desc(tupleDescriptor.toThrift());
        dictQueryExpr.setSchema(schema);
        try {
            dictQueryExpr.setPartition(OlapTableParamGenerator.createPartition(dbId, dictTable, dictTable.getAllPartitionIds()));
            dictQueryExpr.setLocation(
                OlapTableParamGenerator.createLocation(
                    dictTable, dictTable.getClusterId(), dictTable.getAllPartitionIds(), dictTable.enableReplicatedStorage()));
            dictQueryExpr.setNodes_info(OlapTableParamGenerator.createStarrocksNodesInfo(dictTable.getClusterId()));
        } catch (UserException e) {
            SemanticException semanticException = new SemanticException("build OlapTableParams error in dict_query_expr.");
            semanticException.initCause(e);
            throw semanticException;
        }
        Map<Long, Long> partitionVersion = new HashMap<>();
        dictTable.getPartitions().forEach(p -> partitionVersion.put(p.getId(), p.getVisibleVersion()));
        dictQueryExpr.setPartition_version(partitionVersion);

        List<String> keyFields = Arrays.stream(keyFieldExpr.getValue().split(",")).map(String::trim).collect(Collectors.toList());
        String valueField = valueFieldExpr.getValue();
        //todo field name in olap table.
        List<String> columnsNames = dictTable.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());

        for (String f : keyFields) {
            if (!columnsNames.contains(f)) {
                throw new SemanticException("key field '%s' not exist in dict_table.", f);
            }
        }
        if (!columnsNames.contains(valueField)) {
            throw new SemanticException("value field '%s' not exist in dict_table.", valueField);
        }
        dictQueryExpr.setKey_fields(keyFields);
        dictQueryExpr.setValue_field(valueField);
        setType(Type.BIGINT);
        List<Type> ts = Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR);
        this.fn = new Function(FunctionName.createFnName(FunctionSet.DICT_GET), ts, Type.BIGINT, false);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
    }

    public DictQueryExpr(List<Expr> params, TDictQueryExpr dictQueryExpr) {
        super(FunctionSet.DICT_GET, params);
        this.dictQueryExpr = dictQueryExpr;
        List<Type> ts = Arrays.asList(Type.VARCHAR, Type.VARCHAR, Type.VARCHAR, Type.VARCHAR);
        this.fn = new Function(FunctionName.createFnName(FunctionSet.DICT_GET), ts, Type.BIGINT, false);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        setType(Type.BIGINT);
    }

    protected DictQueryExpr(DictQueryExpr other) {
        super(other);
        this.dictQueryExpr = other.getDictQueryExpr();
    }


    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.DICT_QUERY_EXPR);
        msg.setDict_query_expr(dictQueryExpr);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDictQueryExpr(this, context);
    }

    @Override
    public boolean isAggregateFunction() {
        return false;
    }

    @Override
    public Expr clone() {
        return new DictQueryExpr(this);
    }

    public TDictQueryExpr getDictQueryExpr() {
        return dictQueryExpr;
    }
}
