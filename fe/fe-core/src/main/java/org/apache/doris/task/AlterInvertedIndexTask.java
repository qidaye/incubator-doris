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

package org.apache.doris.task;

import org.apache.doris.alter.AlterCancelException;
import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.thrift.TAlterInvertedIndexReq;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TOlapTableIndexSchema;
import org.apache.doris.thrift.TOlapTableSchemaParam;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * This task is used for alter table process, such as rollup and schema change
 * The task will do data transformation from base replica to new replica.
 * The new replica should be created before.
 * The new replica can be a rollup replica, or a shadow replica of schema change.
 */
public class AlterInvertedIndexTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(AlterInvertedIndexTask.class);
    private long tabletId;
    private int schemaHash;
    private List<Index> alterInvertedIndexes;
    private List<Column> schemaColumns;
    private List<Index> existIndexes;
    private boolean isDropOp = false;
    private long jobId;
    private TOlapTableSchemaParam tOlapTableSchemaParam;

    public AlterInvertedIndexTask(long backendId, long dbId, long tableId,
            long partitionId, long indexId, long tabletId, int schemaHash,
            List<Index> existIndexes, List<Index> alterInvertedIndexes,
            List<Column> schemaColumns, boolean isDropOp, long taskSignature,
            long jobId) throws AlterCancelException {
        super(null, backendId, TTaskType.ALTER_INVERTED_INDEX, dbId, tableId,
                partitionId, indexId, tabletId, taskSignature);
        this.tabletId = tabletId;
        this.schemaHash = schemaHash;
        this.existIndexes = existIndexes;
        this.alterInvertedIndexes = alterInvertedIndexes;
        this.schemaColumns = schemaColumns;
        this.isDropOp = isDropOp;
        this.jobId = jobId;
        this.tOlapTableSchemaParam = createSchema();
    }

    public long getTabletId() {
        return tabletId;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public List<Index> getAlterInvertedIndexes() {
        return alterInvertedIndexes;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("");
        if (isDropOp) {
            sb.append("DROP");
        } else {
            sb.append("ADD");
        }
        sb.append(" (");
        for (Index alterIndex : alterInvertedIndexes) {
            sb.append(alterIndex.getIndexId());
            sb.append(": ");
            sb.append(alterIndex.toString());
            sb.append(", ");
        }
        sb.append(" )");
        return sb.toString();
    }

    public TAlterInvertedIndexReq toThrift() {
        TAlterInvertedIndexReq req = new TAlterInvertedIndexReq();
        req.setTabletId(tabletId);
        req.setIndexId(indexId);
        req.setSchemaHash(schemaHash);
        req.setIsDropOp(isDropOp);
        // set jonId for debugging in BE
        req.setJobId(jobId);

        if (!alterInvertedIndexes.isEmpty()) {
            List<TOlapTableIndex> tIndexes = new ArrayList<>();
            for (Index index : alterInvertedIndexes) {
                tIndexes.add(index.toThrift());
            }
            req.setAlterInvertedIndexes(tIndexes);
        }

        if (existIndexes != null) {
            List<TOlapTableIndex> indexDesc = new ArrayList<TOlapTableIndex>();
            for (Index index : existIndexes) {
                TOlapTableIndex tIndex = index.toThrift();
                indexDesc.add(tIndex);
            }
            req.setIndexesDesc(indexDesc);
        }

        if (schemaColumns != null) {
            List<TColumn> columns = new ArrayList<TColumn>();
            for (Column column : schemaColumns) {
                columns.add(column.toThrift());
            }
            req.setColumns(columns);
        }
        req.setSchema(tOlapTableSchemaParam);
        return req;
    }

    private TOlapTableSchemaParam createSchema() throws AlterCancelException {
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrException(dbId, s -> new AlterCancelException("Database " + s + " does not exist"));
        OlapTable table;
        try {
            table = (OlapTable) db.getTableOrMetaException(tableId, TableIf.TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AlterCancelException(e.getMessage());
        }
        Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), null);
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor tupleDescriptor = descTable.createTupleDescriptor("DstTableTuple");

        for (Column col : table.getFullSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDescriptor);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            slotDesc.setAutoInc(col.isAutoInc());
        }

        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDbId(dbId);
        schemaParam.setTableId(table.getId());
        schemaParam.setVersion(table.getIndexMetaByIndexId(table.getBaseIndexId()).getSchemaVersion());
        schemaParam.setIsStrictMode(false);

        schemaParam.tuple_desc = tupleDescriptor.toThrift();
        for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
            schemaParam.addToSlotDescs(slotDesc.toThrift());
        }

        for (Map.Entry<Long, MaterializedIndexMeta> pair : table.getIndexIdToMeta().entrySet()) {
            MaterializedIndexMeta indexMeta = pair.getValue();
            List<String> columns = Lists.newArrayList();
            List<TColumn> columnsDesc = Lists.newArrayList();
            List<TOlapTableIndex> indexDesc = Lists.newArrayList();
            columns.addAll(indexMeta.getSchema().stream().map(Column::getNonShadowName).collect(Collectors.toList()));
            for (Column column : indexMeta.getSchema()) {
                TColumn tColumn = column.toThrift();
                // When schema change is doing, some modified column has prefix in name. Columns here
                // is for the schema in rowset meta, which should be no column with shadow prefix.
                // So we should remove the shadow prefix here.
                if (column.getName().startsWith(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                    tColumn.setColumnName(column.getNonShadowName());
                }
                column.setIndexFlag(tColumn, table);
                columnsDesc.add(tColumn);
            }
            List<Index> indexes = indexMeta.getIndexes();
            if (indexes.size() == 0 && pair.getKey() == table.getBaseIndexId()) {
                // for compatible with old version befor 2.0-beta
                // if indexMeta.getIndexes() is empty, use table.getIndexes()
                indexes = table.getIndexes();
            }
            for (Index index : indexes) {
                TOlapTableIndex tIndex = index.toThrift();
                indexDesc.add(tIndex);
            }
            TOlapTableIndexSchema indexSchema = new TOlapTableIndexSchema(pair.getKey(), columns,
                    indexMeta.getSchemaHash());
            indexSchema.setColumnsDesc(columnsDesc);
            indexSchema.setIndexesDesc(indexDesc);
            schemaParam.addToIndexes(indexSchema);
        }
        schemaParam.setIsPartialUpdate(false);
        return schemaParam;
    }
}
