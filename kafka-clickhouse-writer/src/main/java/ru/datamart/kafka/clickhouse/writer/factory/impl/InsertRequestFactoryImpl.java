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

import io.vertx.core.json.JsonArray;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.datamart.kafka.clickhouse.util.DateTimeUtils;
import ru.datamart.kafka.clickhouse.writer.configuration.properties.EnvProperties;
import ru.datamart.kafka.clickhouse.writer.factory.InsertRequestFactory;
import ru.datamart.kafka.clickhouse.writer.model.InsertDataContext;
import ru.datamart.kafka.clickhouse.writer.model.sql.ClickhouseInsertSqlRequest;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
public class InsertRequestFactoryImpl implements InsertRequestFactory {
    private static final String TEMPLATE_SQL = "insert into %s.%s (%s) values (%s)";
    private final EnvProperties envProperties;

    @Autowired
    public InsertRequestFactoryImpl(EnvProperties envProperties) {
        this.envProperties = envProperties;
    }

    @Override
    public ClickhouseInsertSqlRequest create(InsertDataContext context, List<GenericRecord> rows) {
        if (!rows.isEmpty()) {
            return createSqlRequest(context, rows);
        } else return new ClickhouseInsertSqlRequest(context.getInsertSql(), new ArrayList<>());
    }

    private ClickhouseInsertSqlRequest createSqlRequest(InsertDataContext context, List<GenericRecord> rows) {
        val fields = rows.get(0).getSchema().getFields();
        val batch = getParams(rows, fields);
        return new ClickhouseInsertSqlRequest(context.getInsertSql(), batch);
    }

    private List<JsonArray> getParams(List<GenericRecord> rows, List<Schema.Field> fields) {
        return rows.stream()
                .map(row -> getJsonParam(fields, row))
                .collect(Collectors.toList());
    }

    private JsonArray getJsonParam(List<Schema.Field> fields, GenericRecord row) {
        val tuple = new JsonArray();
        IntStream.range(0, fields.size()).mapToObj(row::get).forEach(f -> {
            tuple.add(convertValueIfNeeded(f));
        });
        return tuple;
    }

    private Object convertValueIfNeeded(Object value) {
        if (value instanceof LocalDate) {
            return DateTimeUtils.toEpochDay((LocalDate) value);
        } else if (value instanceof LocalTime) {
            return DateTimeUtils.toMicros((LocalTime) value);
        } else if (value instanceof LocalDateTime) {
            return DateTimeUtils.toMicros((LocalDateTime) value);
        }
        return value;
    }

    @Override
    public String getSql(InsertDataContext context) {
        val fields = context.getRequest().getSchema().getFields();
        return getInsertSql(context, fields);
    }

    private String getInsertSql(InsertDataContext context, List<Schema.Field> fields) {
        val insertColumns = fields.stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());
        val insertValues = IntStream.range(1, fields.size() + 1)
                .mapToObj(pos -> "?")
                .collect(Collectors.toList());
        val request = context.getRequest();
        val schema = request.isWriteIntoDistributedTable() ? request.getDatamart()
                : envProperties.getName() + envProperties.getDelimiter() + request.getDatamart();
        val table = request.isWriteIntoDistributedTable() ? request.getTableName()
                : request.getTableName() + "_ext_shard";
        return String.format(TEMPLATE_SQL, schema, table, String.join(",", insertColumns),
                String.join(",", insertValues));
    }
}
