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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/OlapTableSink.java

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

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.load.Load;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TNodeInfo;
import com.starrocks.thrift.TNodesInfo;
import com.starrocks.thrift.TOlapTableIndexSchema;
import com.starrocks.thrift.TOlapTableIndexTablets;
import com.starrocks.thrift.TOlapTableLocationParam;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TOlapTableSchemaParam;
import com.starrocks.thrift.TOlapTableSink;
import com.starrocks.thrift.TTabletLocation;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.transaction.TransactionState;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class OlapTableSink extends DataSink {
    private static final Logger LOG = LogManager.getLogger(OlapTableSink.class);

    private final int clusterId;
    // input variables
    private OlapTable dstTable;
    private final TupleDescriptor tupleDescriptor;
    // specified partition ids. this list should not be empty and should contains all related partition ids
    private List<Long> partitionIds;

    // set after init called
    private TDataSink tDataSink;

    private final boolean enablePipelineLoad;
    private final TWriteQuorumType writeQuorum;
    private final boolean enableReplicatedStorage;

    private boolean nullExprInAutoIncrement;
    private boolean missAutoIncrementColumn;
    private boolean abortDelete;
    private int autoIncrementSlotId;

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
            TWriteQuorumType writeQuorum, boolean enableReplicatedStorage, boolean nullExprInAutoIncrement) {
        this(dstTable, tupleDescriptor, partitionIds, true, writeQuorum, enableReplicatedStorage, nullExprInAutoIncrement);
    }

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
            boolean enablePipelineLoad, TWriteQuorumType writeQuorum, boolean enableReplicatedStorage,
            boolean nullExprInAutoIncrement) {
        this.dstTable = dstTable;
        this.tupleDescriptor = tupleDescriptor;
        Preconditions.checkState(!CollectionUtils.isEmpty(partitionIds));
        this.partitionIds = partitionIds;
        this.clusterId = dstTable.getClusterId();
        this.enablePipelineLoad = enablePipelineLoad;
        this.writeQuorum = writeQuorum;
        this.enableReplicatedStorage = enableReplicatedStorage;
        this.nullExprInAutoIncrement = nullExprInAutoIncrement;
        this.missAutoIncrementColumn = false;
        this.abortDelete = dstTable.isAbortDelete();

        this.autoIncrementSlotId = -1;
        if (tupleDescriptor != null) {
            for (int i = 0; i < this.tupleDescriptor.getSlots().size(); ++i) {
                SlotDescriptor slot = this.tupleDescriptor.getSlots().get(i);
                if (slot.getColumn().isAutoIncrement()) {
                    this.autoIncrementSlotId = i;
                    break;
                }
            }
        }
    }

    public void init(TUniqueId loadId, long txnId, long dbId, long loadChannelTimeoutS)
            throws AnalysisException {
        TOlapTableSink tSink = new TOlapTableSink();
        tSink.setLoad_id(loadId);
        tSink.setTxn_id(txnId);
        tSink.setNull_expr_in_auto_increment(nullExprInAutoIncrement);
        tSink.setMiss_auto_increment_column(missAutoIncrementColumn);
        tSink.setAbort_delete(abortDelete);
        tSink.setAuto_increment_slot_id(autoIncrementSlotId);
        TransactionState txnState =
                GlobalStateMgr.getCurrentGlobalTransactionMgr()
                        .getTransactionState(dbId, txnId);
        if (txnState != null) {
            tSink.setTxn_trace_parent(txnState.getTraceParent());
        }
        tSink.setDb_id(dbId);
        tSink.setLoad_channel_timeout_s(loadChannelTimeoutS);
        tSink.setIs_lake_table(dstTable.isLakeTable());
        tSink.setKeys_type(dstTable.getKeysType().toThrift());
        tSink.setWrite_quorum_type(writeQuorum);
        tSink.setEnable_replicated_storage(enableReplicatedStorage);
        tDataSink = new TDataSink(TDataSinkType.DATA_SPLIT_SINK);
        tDataSink.setType(TDataSinkType.OLAP_TABLE_SINK);
        tDataSink.setOlap_table_sink(tSink);

        for (Long partitionId : partitionIds) {
            Partition part = dstTable.getPartition(partitionId);
            if (part == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PARTITION, partitionId, dstTable.getName());
            }
        }
    }

    public void setMissAutoIncrementColumn() {
        this.missAutoIncrementColumn = true;
    }

    public void updateLoadId(TUniqueId newLoadId) {
        tDataSink.getOlap_table_sink().setLoad_id(newLoadId);
    }

    public void complete(String mergeCondition) throws UserException {
        TOlapTableSink tSink = tDataSink.getOlap_table_sink();
        if (mergeCondition != null && !mergeCondition.isEmpty()) {
            tSink.setMerge_condition(mergeCondition);
        }
        complete();
    }

    // must called after tupleDescriptor is computed
    public void complete() throws UserException {
        TOlapTableSink tSink = tDataSink.getOlap_table_sink();

        tSink.setTable_id(dstTable.getId());
        tSink.setTable_name(dstTable.getName());
        tSink.setTuple_id(tupleDescriptor.getId().asInt());
        int numReplicas = 1;
        Optional<Partition> optionalPartition = dstTable.getPartitions().stream().findFirst();
        if (optionalPartition.isPresent()) {
            long partitionId = optionalPartition.get().getId();
            numReplicas = dstTable.getPartitionInfo().getReplicationNum(partitionId);
        }
        tSink.setNum_replicas(numReplicas);
        tSink.setNeed_gen_rollup(dstTable.shouldLoadToNewRollup());
        tSink.setSchema(createSchema(tSink.getDb_id(), dstTable));
        tSink.setPartition(createPartition(tSink.getDb_id(), dstTable));
        tSink.setLocation(createLocation(dstTable));
        tSink.setNodes_info(createStarrocksNodesInfo());
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "OLAP TABLE SINK\n");
        strBuilder.append(prefix + "  TABLE: " + dstTable.getName() + "\n");
        strBuilder.append(prefix + "  TUPLE ID: " + tupleDescriptor.getId() + "\n");
        strBuilder.append(prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel));
        return strBuilder.toString();
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

    @Override
    public TDataSink toThrift() {
        return tDataSink;
    }

    private TOlapTableSchemaParam createSchema(long dbId, OlapTable table) {
        final TOlapTableSchemaParam schemaParam = OlapTableParamGenerator.createSchema(dbId, table);
        schemaParam.tuple_desc = tupleDescriptor.toThrift();
        for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
            schemaParam.addToSlot_descs(slotDesc.toThrift());
        }
        return schemaParam;
    }

    private TOlapTablePartitionParam createPartition(long dbId, OlapTable table) throws UserException {
        return OlapTableParamGenerator.createPartition(dbId, table, partitionIds);
    }

    private TOlapTableLocationParam createLocation(OlapTable table) throws UserException {
        return OlapTableParamGenerator.createLocation(table, clusterId, partitionIds, enableReplicatedStorage);
    }

    private TNodesInfo createStarrocksNodesInfo() {
        return OlapTableParamGenerator.createStarrocksNodesInfo(clusterId);
    }

    public boolean canUsePipeLine() {
        return Config.enable_pipeline_load && enablePipelineLoad;
    }

    public int getClusterId() {
        return clusterId;
    }

    public OlapTable getDstTable() {
        return dstTable;
    }

    public void setDstTable(OlapTable table) {
        this.dstTable = table;
    }

    public void setPartitionIds(List<Long> partitionIds) {
        this.partitionIds = partitionIds;
    }

    public TupleDescriptor getTupleDescriptor() {
        return tupleDescriptor;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public TWriteQuorumType getWriteQuorum() {
        return writeQuorum;
    }

    public boolean isEnableReplicatedStorage() {
        return enableReplicatedStorage;
    }

    public boolean missAutoIncrementColumn() {
        return this.missAutoIncrementColumn;
    }
}

