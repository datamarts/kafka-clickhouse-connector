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
package ru.datamart.kafka.clickhouse.reader.converter;

import ru.datamart.kafka.clickhouse.reader.converter.transformer.ColumnTransformer;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("clickhouseTypeToSqlTypeConverter")
public class ClickhouseTypeToSqlTypeConverter implements SqlTypeConverter {

    private final Map<Schema.Type, Map<Class<?>, ColumnTransformer>> transformerMap;

    @Autowired
    public ClickhouseTypeToSqlTypeConverter(@Qualifier("clickhouseTransformerMap")
                                                    Map<Schema.Type, Map<Class<?>, ColumnTransformer>> transformerMap) {
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<Schema.Type, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }

}
