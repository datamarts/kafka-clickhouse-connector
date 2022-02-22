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

import java.util.Map;

public interface SqlTypeConverter {

    default Object convert(Schema.Type type, Object value) {
        if (value == null) {
            return null;
        }
        final Map<Class<?>, ColumnTransformer> transformerClassMap = getTransformerMap().get(type);
        if (transformerClassMap != null && !transformerClassMap.isEmpty()) {
            final ColumnTransformer columnTransformer = transformerClassMap.get(value.getClass());
            if (columnTransformer != null) {
                return columnTransformer.transform(value);
            } else {
                try {
                    return transformerClassMap.get(Object.class).transform(value);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Can't transform value for column type [%s] and class [%s]",
                        type, value.getClass()), e);
                }
            }
        } else {
            throw new RuntimeException(String.format("Can't find transformers for type [%s]", type));
        }
    }

    Map<Schema.Type, Map<Class<?>, ColumnTransformer>> getTransformerMap();
}
