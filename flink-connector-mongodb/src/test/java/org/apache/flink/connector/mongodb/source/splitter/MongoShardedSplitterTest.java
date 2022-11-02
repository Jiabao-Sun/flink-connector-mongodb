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

package org.apache.flink.connector.mongodb.source.splitter;

import org.apache.flink.connector.mongodb.common.utils.MongoUtils;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.MongoShardedSplitter;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.MongoSplitContext;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplit;
import org.apache.flink.util.TestLoggerExtension;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.AVG_OBJ_SIZE_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.COUNT_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.DROPPED_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_HINT;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.KEY_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MAX_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.MIN_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.SHARD_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.SIZE_FIELD;
import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.UUID_FIELD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/** Unit tests for {@link MongoShardedSplitter}. */
@ExtendWith(TestLoggerExtension.class)
public class MongoShardedSplitterTest {

    @Mock private MongoClient mongoClient;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testShardedSplitter() {
        MongoNamespace namespace = new MongoNamespace("test_db.test_coll");
        BsonDocument mockCollectionMetadata = mockCollectionMetadata();
        ArrayList<BsonDocument> mockChunksData = mockChunksData();

        MongoSplitContext splitContext =
                MongoSplitContext.of(
                        MongoReadOptions.builder().build(),
                        mongoClient,
                        namespace,
                        mockCollStats());

        List<MongoSourceSplit> expected = new ArrayList<>();
        for (int i = 0; i < mockChunksData.size(); i++) {
            BsonDocument mockChunkData = mockChunksData.get(i);
            expected.add(
                    new MongoSourceSplit(
                            String.format("%s_%d", namespace, i),
                            namespace.getDatabaseName(),
                            namespace.getCollectionName(),
                            mockChunkData.getDocument(MIN_FIELD),
                            mockChunkData.getDocument(MAX_FIELD),
                            mockCollectionMetadata.getDocument(KEY_FIELD)));
        }

        try (MockedStatic<MongoUtils> util = mockStatic(MongoUtils.class)) {
            util.when(() -> MongoUtils.readCollectionMetadata(any(), any()))
                    .thenReturn(mockCollectionMetadata);

            util.when(() -> MongoUtils.readChunks(any(), any())).thenReturn(mockChunksData);

            util.when(() -> MongoUtils.isValidShardedCollection(any())).thenReturn(true);

            Collection<MongoSourceSplit> actual =
                    MongoShardedSplitter.INSTANCE.split(splitContext);
            assertThat(actual, equalTo(expected));
        }
    }

    private BsonDocument mockCollectionMetadata() {
        return new BsonDocument()
                .append(ID_FIELD, new BsonObjectId())
                .append(UUID_FIELD, new BsonBinary(UUID.randomUUID()))
                .append(DROPPED_FIELD, BsonBoolean.FALSE)
                .append(KEY_FIELD, ID_HINT);
    }

    private ArrayList<BsonDocument> mockChunksData() {
        ArrayList<BsonDocument> chunks = new ArrayList<>();
        chunks.add(mockChunkData(1));
        chunks.add(mockChunkData(2));
        chunks.add(mockChunkData(3));
        return chunks;
    }

    private BsonDocument mockChunkData(int index) {
        return new BsonDocument()
                .append(MIN_FIELD, new BsonDocument(ID_FIELD, new BsonInt32(index * 100)))
                .append(MAX_FIELD, new BsonDocument(ID_FIELD, new BsonInt32((index + 1) * 100)))
                .append(SHARD_FIELD, new BsonString("shard-" + index));
    }

    private BsonDocument mockCollStats() {
        return new BsonDocument()
                .append(SHARD_FIELD, BsonBoolean.TRUE)
                .append(COUNT_FIELD, new BsonInt64(10000L))
                .append(SIZE_FIELD, new BsonInt64(10000L))
                .append(AVG_OBJ_SIZE_FIELD, new BsonInt64(1L));
    }
}
