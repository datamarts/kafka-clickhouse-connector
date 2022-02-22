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

import ru.datamart.kafka.clickhouse.reader.configuration.properties.VerticleProperties;
import ru.datamart.kafka.clickhouse.reader.factory.UpstreamFactory;
import ru.datamart.kafka.clickhouse.reader.model.QueryRequest;
import ru.datamart.kafka.clickhouse.reader.model.QueryResultItem;
import ru.datamart.kafka.clickhouse.reader.service.DatabaseExecutor;
import ru.datamart.kafka.clickhouse.reader.upstream.Upstream;
import ru.datamart.kafka.clickhouse.reader.verticle.TaskVerticleExecutorImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class ClickhouseQueryExecutorServiceTest {
    @Mock
    private DatabaseExecutor databaseExecutor;

    @Mock
    private UpstreamFactory<QueryResultItem> upstreamFactory;

    @Mock
    private Upstream<QueryResultItem> upstream;

    @Mock
    private QueryResultItem queryResultItem;

    private List<DatabaseExecutor> databases;
    private ClickhouseQueryExecutorService executorService;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        when(upstreamFactory.create(any(), any())).thenReturn(upstream);

        databases = Arrays.asList(databaseExecutor, databaseExecutor);
        VerticleProperties properties = new VerticleProperties();
        properties.setTaskWorker(new VerticleProperties.WorkerProperties(2, "pool", 100000L));
        TaskVerticleExecutorImpl verticle = new TaskVerticleExecutorImpl(properties);
        executorService = new ClickhouseQueryExecutorService(databases, upstreamFactory, verticle);
        vertx.deployVerticle(verticle, ar -> testContext.completeNow());
    }

    @Test
    void shouldRunAlDatabaseExecutorsWhenParallel(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.execute(any(), any())).thenAnswer(invocation -> {
            Consumer<QueryResultItem> messageProcessor = invocation.getArgument(1);
            messageProcessor.accept(queryResultItem);
            return Future.succeededFuture();
        });
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(upstream).push(any(), any(), any());

        val request = QueryRequest.builder().parallelMode(true).build();

        // act
        executorService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor, times(2)).execute(any(), any());
                    verify(upstreamFactory, times(2)).create(any(), any());
                    verify(upstream, times(2)).push(any(), any(), any());
                    verify(upstream, times(2)).close();
                    verifyNoMoreInteractions(databaseExecutor, upstream);
                }).completeNow());
    }

    @Test
    void shouldRunSingleDatabaseExecutorWhenNotParallel(VertxTestContext testContext) {
        // arrange
        when(databaseExecutor.execute(any(), any())).thenAnswer(invocation -> {
            Consumer<QueryResultItem> messageProcessor = invocation.getArgument(1);
            messageProcessor.accept(queryResultItem);
            return Future.succeededFuture();
        });
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(upstream).push(any(), any(), any());

        val request = QueryRequest.builder().parallelMode(false).build();

        // act
        executorService.execute(request)
                .onComplete(ar -> testContext.verify(() -> {
                    // assert
                    if (ar.failed()) {
                        fail(ar.cause());
                    }

                    verify(databaseExecutor).execute(any(), any());
                    verify(upstream).push(any(), any(), any());
                    verify(upstream).close();
                    verifyNoMoreInteractions(databaseExecutor, upstream);
                }).completeNow());
    }

}