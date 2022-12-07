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

package org.apache.flink.connector.mongodb.source.enumerator.splitter;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.connector.mongodb.common.utils.MongoConstants.ID_FIELD;

/** Paginates a collection into partitions. */
public class MongoPaginateSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoPaginateSplitter.class);

    public static final MongoPaginateSplitter INSTANCE = new MongoPaginateSplitter();

    private MongoPaginateSplitter() {}

    public Collection<MongoScanSourceSplit> split(MongoSplitContext splitContext) {
        MongoReadOptions readOptions = splitContext.getReadOptions();
        MongoNamespace namespace = splitContext.getMongoNamespace();

        long count = splitContext.getCount();
        long partitionSizeInBytes = readOptions.getPartitionSize().getBytes();

        double avgObjSizeInBytes = splitContext.getAvgObjSize();
        if (avgObjSizeInBytes == 0.0d) {
            LOG.info(
                    "{} seems to be an empty collection, Returning a single partition.", namespace);
            return MongoSingleSplitter.INSTANCE.split(splitContext);
        }

        int numDocumentsPerPartition = (int) Math.floor(partitionSizeInBytes / avgObjSizeInBytes);
        if (numDocumentsPerPartition >= count) {
            LOG.info(
                    "Fewer documents ({}) than the number of documents per partition ({}), Returning a single partition.",
                    count,
                    numDocumentsPerPartition);
            return MongoSingleSplitter.INSTANCE.split(splitContext);
        }

        Bson projection =  Projections.include(ID_FIELD);
        List<Bson> aggregationPipeline = new ArrayList<>();
        aggregationPipeline.add(Aggregates.project(projection));
        aggregationPipeline.add(Aggregates.sort(Sorts.ascending(ID_FIELD)));

        long numberOfPartitions = (count / numDocumentsPerPartition) + 1;

        List<BsonDocument> upperBounds = new ArrayList<>();
        for (int i = 0; i < numberOfPartitions; i++) {
            List<Bson> boundaryPipeline = new ArrayList<>();

            // Uses the previous boundary as the $gte match to efficiently skip to the next bounds.
            if (!upperBounds.isEmpty()) {
                BsonDocument previous = upperBounds.get(upperBounds.size() - 1);
                BsonDocument matchFilter = new BsonDocument();
                if (previous.containsKey(ID_FIELD)) {
                    matchFilter.put(
                            ID_FIELD, new BsonDocument("$gte", previous.get(ID_FIELD)));
                }
                boundaryPipeline.add(Aggregates.match(matchFilter));
            }
            boundaryPipeline.addAll(aggregationPipeline);
            boundaryPipeline.add(Aggregates.skip(numDocumentsPerPartition));
            boundaryPipeline.add(Aggregates.limit(1));

            BsonDocument boundary = splitContext
                    .getMongoCollection()
                    .aggregate(boundaryPipeline)
                    .allowDiskUse(true)
                    .first();

            if (boundary == null) {
                break;
            }
            upperBounds.add(boundary);
        }

        return upperBounds;
    }
}
