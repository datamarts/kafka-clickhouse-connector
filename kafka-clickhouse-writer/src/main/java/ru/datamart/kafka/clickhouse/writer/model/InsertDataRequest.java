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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import org.apache.avro.Schema;
import ru.datamart.kafka.clickhouse.avro.SchemaDeserializer;
import ru.datamart.kafka.clickhouse.avro.SchemaSerializer;
import ru.datamart.kafka.clickhouse.writer.model.kafka.KafkaBrokerInfo;

import java.io.Serializable;
import java.util.List;

@Data
public class InsertDataRequest implements Serializable {
    private String requestId;
    private String datamart;
    private String tableName;
    private List<KafkaBrokerInfo> kafkaBrokers;
    private String kafkaTopic;
    private String consumerGroup;
    private String format;
    @JsonDeserialize(using = SchemaDeserializer.class)
    @JsonSerialize(using = SchemaSerializer.class)
    private Schema schema;
    private int messageProcessingLimit;
    private boolean writeIntoDistributedTable;
}
