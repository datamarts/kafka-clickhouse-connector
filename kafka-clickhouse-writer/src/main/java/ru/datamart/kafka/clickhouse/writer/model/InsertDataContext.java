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
package ru.datamart.kafka.clickhouse.writer.model;

import io.vertx.ext.web.RoutingContext;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class InsertDataContext {
    private final AtomicInteger workingMessagesNumber;
    private final AtomicLong rowsCounter;
    private final InsertDataRequest request;
    private final String contextId;
    private List<String> verticleIds = new ArrayList<>();
    private RoutingContext context;
    private long startTime;
    private List<String> keyColumns;
    private List<String> columnsList;
    private String selectPrimaryKeysSql;
    private String existsSql;
    private String insertSql;
    private Throwable cause;

    public InsertDataContext(InsertDataRequest request, RoutingContext context) {
        Objects.requireNonNull(request);
        Objects.requireNonNull(context);
        this.request = request;
        this.context = context;
        workingMessagesNumber = new AtomicInteger(0);
        rowsCounter = new AtomicLong(0);
        startTime = System.currentTimeMillis();
        this.contextId = InsertDataContext.createContextId(request.getKafkaTopic(), request.getRequestId());
    }

    public static String createContextId(String kafkaTopic, String requestId) {
        return kafkaTopic + requestId;
    }

    public long getProcessingTime() {
        return System.currentTimeMillis() - startTime;
    }

    public InsertDataResult toNewDataResult() {
        return new InsertDataResult(rowsCounter.get(), columnsList, keyColumns, cause);
    }
}
