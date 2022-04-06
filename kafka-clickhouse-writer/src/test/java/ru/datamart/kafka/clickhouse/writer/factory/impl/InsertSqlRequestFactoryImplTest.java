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
package ru.datamart.kafka.clickhouse.writer.factory.impl;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;
import lombok.val;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.datamart.kafka.clickhouse.writer.configuration.properties.EnvProperties;
import ru.datamart.kafka.clickhouse.writer.model.InsertDataContext;
import ru.datamart.kafka.clickhouse.writer.model.InsertDataRequest;
import ru.datamart.kafka.clickhouse.writer.model.sql.ClickhouseInsertSqlRequest;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InsertSqlRequestFactoryImplTest {
    public static final int EXPECTED_DELTA = 10;
    private static final String EXPECTED_SQL = "insert into local__test_datamart.test_table_ext_shard (id,name) values (?,?)";
    private InsertRequestFactoryImpl factory;
    private InsertDataContext context;
    private final RoutingContext routingContext = mock(RoutingContext.class);

    @BeforeEach
    public void before() {
        EnvProperties envProperties = new EnvProperties();
        envProperties.setName("local");
        factory = new InsertRequestFactoryImpl(envProperties);

        HttpServerRequest httpServerRequest = mock(HttpServerRequest.class);
        when(routingContext.request()).thenReturn(httpServerRequest);
        initContext();
    }

    private void initContext() {
        val request = new InsertDataRequest();
        request.setDatamart("test_datamart");
        request.setKafkaTopic("kafka_topic");
        request.setTableName("test_table");
        request.setSchema(SchemaBuilder.record("test").fields()
                .optionalInt("id")
                .name("name")
                .type()
                .nullable()
                .stringBuilder()
                .prop("avro.java.string", "String")
                .endString()
                .noDefault()
                .endRecord());
        context = new InsertDataContext(request, routingContext);
        context.setKeyColumns(Arrays.asList("id", "name"));
    }

    @Test
    void createInsertRequest() {
        context.setInsertSql(factory.getSql(context));
        ClickhouseInsertSqlRequest request = factory.create(context, getRowsWithoutDelta());
        assertEquals(EXPECTED_SQL, request.getSql());
        assertEquals(10, request.getParams().size());
        JsonArray tuple = request.getParams().get(0);
        assertEquals((Integer) 2, tuple.size());
    }

    private List<GenericRecord> getRowsWithoutDelta() {
        return IntStream.range(0, 10)
                .mapToObj(it -> getRowWithoutDelta(it, "name_" + it))
                .collect(Collectors.toList());
    }

    private GenericData.Record getRowWithoutDelta(int expectedId, String expectedName) {
        val schema = SchemaBuilder.record("test").fields()
                .optionalString("id")
                .optionalString("name")
                .endRecord();
        return new GenericRecordBuilder(schema)
                .set("id", expectedId)
                .set("name", expectedName)
                .build();
    }

}
