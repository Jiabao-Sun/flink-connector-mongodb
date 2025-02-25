/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.sink.config.MongoWriteOptions;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters.RowDataToBsonConverter;
import org.apache.flink.connector.mongodb.table.serialization.MongoRowDataSerializationSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.SerializableFunction;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link DynamicTableSink} for MongoDB. */
@Internal
public class MongoDynamicTableSink
        implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDynamicTableSink.class);

    private final MongoConnectionOptions connectionOptions;
    private final MongoWriteOptions writeOptions;
    @Nullable private final Integer parallelism;
    private final boolean supportUpsert;
    private final ResolvedSchema resolvedSchema;
    private final String[] partitionKeys;
    private final SerializableFunction<RowData, BsonValue> primaryKeyExtractor;
    private final SerializableFunction<RowData, BsonDocument> shardKeysExtractor;

    public MongoDynamicTableSink(
            MongoConnectionOptions connectionOptions,
            MongoWriteOptions writeOptions,
            @Nullable Integer parallelism,
            ResolvedSchema resolvedSchema,
            String[] partitionKeys) {
        this.connectionOptions = checkNotNull(connectionOptions);
        this.writeOptions = checkNotNull(writeOptions);
        this.parallelism = parallelism;
        this.resolvedSchema = checkNotNull(resolvedSchema);
        this.partitionKeys = checkNotNull(partitionKeys);
        this.supportUpsert = resolvedSchema.getPrimaryKey().isPresent();
        this.primaryKeyExtractor =
                MongoPrimaryKeyExtractor.createPrimaryKeyExtractor(resolvedSchema);
        this.shardKeysExtractor =
                MongoShardKeysExtractor.createShardKeysExtractor(resolvedSchema, partitionKeys);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (supportUpsert) {
            return ChangelogMode.upsert();
        } else {
            return ChangelogMode.insertOnly();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowDataToBsonConverter rowDataToBsonConverter =
                RowDataToBsonConverters.createConverter(
                        (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType());

        final MongoRowDataSerializationSchema serializationSchema =
                new MongoRowDataSerializationSchema(
                        rowDataToBsonConverter, primaryKeyExtractor, shardKeysExtractor);

        final MongoSink<RowData> mongoSink =
                MongoSink.<RowData>builder()
                        .setUri(connectionOptions.getUri())
                        .setDatabase(connectionOptions.getDatabase())
                        .setCollection(connectionOptions.getCollection())
                        .setBatchSize(writeOptions.getBatchSize())
                        .setBatchIntervalMs(writeOptions.getBatchIntervalMs())
                        .setDeliveryGuarantee(writeOptions.getDeliveryGuarantee())
                        .setMaxRetries(writeOptions.getMaxRetries())
                        .setSerializationSchema(serializationSchema)
                        .build();

        return SinkV2Provider.of(mongoSink, parallelism);
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // The value of the partition keys is obtained at runtime, just print static partition here.
        LOG.info("Applied static partition: {}", partition);
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        if (overwrite && !supportUpsert) {
            throw new IllegalStateException(
                    "Overwrite sink requires specifying the table's primary key");
        }
    }

    @Override
    public MongoDynamicTableSink copy() {
        return new MongoDynamicTableSink(
                connectionOptions, writeOptions, parallelism, resolvedSchema, partitionKeys);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDynamicTableSink that = (MongoDynamicTableSink) o;
        return Objects.equals(connectionOptions, that.connectionOptions)
                && Objects.equals(writeOptions, that.writeOptions)
                && Objects.equals(parallelism, that.parallelism)
                && Objects.equals(supportUpsert, that.supportUpsert)
                && Objects.equals(resolvedSchema, that.resolvedSchema)
                && Arrays.equals(partitionKeys, that.partitionKeys);
    }

    @Override
    public int hashCode() {
        return 31
                        * Objects.hash(
                                connectionOptions,
                                writeOptions,
                                parallelism,
                                supportUpsert,
                                resolvedSchema)
                + Arrays.hashCode(partitionKeys);
    }
}
