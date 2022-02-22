/*
 * Copyright © 2022 DATAMART LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.kafka.clickhouse.reader.service.impl;

import ru.datamart.kafka.clickhouse.reader.factory.UpstreamFactory;
import ru.datamart.kafka.clickhouse.reader.model.QueryRequest;
import ru.datamart.kafka.clickhouse.reader.model.QueryResultItem;
import ru.datamart.kafka.clickhouse.reader.service.DatabaseExecutor;
import ru.datamart.kafka.clickhouse.reader.service.QueryExecutorService;
import ru.datamart.kafka.clickhouse.reader.upstream.Upstream;
import ru.datamart.kafka.clickhouse.reader.verticle.TaskVerticleExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
@Slf4j
public class ClickhouseQueryExecutorService implements QueryExecutorService {
    private final Random random = new Random();
    private final List<DatabaseExecutor> databaseExecutors;
    private final UpstreamFactory<QueryResultItem> upstreamFactory;
    private final TaskVerticleExecutor taskExecutor;

    @Autowired
    public ClickhouseQueryExecutorService(@Qualifier("clickhouseDatabaseExecutorList") List<DatabaseExecutor> databaseExecutors,
                                          UpstreamFactory<QueryResultItem> upstreamFactory,
                                          TaskVerticleExecutor taskExecutor) {
        this.databaseExecutors = databaseExecutors;
        this.upstreamFactory = upstreamFactory;
        this.taskExecutor = taskExecutor;
    }

    @Override
    public Future<Void> execute(QueryRequest query) {
        return Future.future(p -> {
            List<Future> queryFutures = new ArrayList<>();

            if (query.isParallelMode()) {
                val totalExecutors = databaseExecutors.size();
                for (int i = 0; i < totalExecutors; i++) {
                    val databaseExecutor = databaseExecutors.get(i);
                    queryFutures.add(prepareQueryFuture(query, databaseExecutor, i, totalExecutors));
                }
            } else {
                val randomDatabaseExecutor = databaseExecutors.get(random.nextInt(databaseExecutors.size()));
                queryFutures.add(prepareQueryFuture(query, randomDatabaseExecutor, 0, 1));
            }

            CompositeFuture.join(queryFutures)
                    .onSuccess(success -> {
                        log.debug("Query executed successfully {}", query);
                        p.complete();
                    })
                    .onFailure(fail -> {
                        log.error("Error in executing query {}", query, fail);
                        p.fail(fail);
                    });
        });
    }

    private Future<Void> prepareQueryFuture(QueryRequest query, DatabaseExecutor databaseExecutor, int executorIndex, int totalExecutors) {
        val queryRequest = query.copy();
        queryRequest.setStreamTotal(totalExecutors);
        queryRequest.setStreamNumber(executorIndex);
        Handler<Promise<Void>> codeHandler = prepareExecuteQueryTask(databaseExecutor, queryRequest);
        return taskExecutor.execute(codeHandler);
    }

    private Handler<Promise<Void>> prepareExecuteQueryTask(DatabaseExecutor executor,
                                                           QueryRequest query) {
        val upstream = upstreamFactory.create(query.getAvroSchema(), query.getKafkaBrokers());
        List<Future> messageFutures = new CopyOnWriteArrayList<>();
        return ((Promise<Void> p) -> {
            executor.execute(query, item -> messageFutures.add(pushMessage(query, upstream, item)))
                    .compose(unused -> CompositeFuture.join(messageFutures))
                    .onComplete(ar -> {
                        upstream.close();
                        if (ar.succeeded()) {
                            p.complete();
                        } else {
                            p.fail(ar.cause());
                        }
                    });
        });
    }

    private Future<Void> pushMessage(QueryRequest query, Upstream<QueryResultItem> upstream,
                                     QueryResultItem resultItem) {
        return Future.future(promise ->
                upstream.push(query, resultItem, ur -> {
                    if (ur.succeeded()) {
                        log.debug("Chunk [{}] for table [{}] was pushed successfully into topic [{}] isLast [{}]",
                                resultItem.getChunkNumber(),
                                resultItem.getTable(),
                                resultItem.getKafkaTopic(),
                                resultItem.getIsLastChunk());
                        promise.complete();
                    } else {
                        log.error("Error sending message to kafka by query {}", query, ur.cause());
                        promise.fail(ur.cause());
                    }
                }));
    }
}
