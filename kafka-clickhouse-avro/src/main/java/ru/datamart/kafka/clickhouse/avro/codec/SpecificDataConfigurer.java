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
package ru.datamart.kafka.clickhouse.avro.codec;

import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import ru.datamart.kafka.clickhouse.avro.codec.conversion.BigDecimalConversion;
import ru.datamart.kafka.clickhouse.avro.codec.conversion.LocalDateConversion;
import ru.datamart.kafka.clickhouse.avro.codec.conversion.LocalDateTimeConversion;
import ru.datamart.kafka.clickhouse.avro.codec.conversion.LocalTimeConversion;
import ru.datamart.kafka.clickhouse.avro.codec.type.BigDecimalLogicalType;
import ru.datamart.kafka.clickhouse.avro.codec.type.LocalDateLogicalType;
import ru.datamart.kafka.clickhouse.avro.codec.type.LocalDateTimeLogicalType;
import ru.datamart.kafka.clickhouse.avro.codec.type.LocalTimeLogicalType;

public abstract class SpecificDataConfigurer {
    static {
        GenericData.get().addLogicalTypeConversion(LocalDateConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(LocalTimeConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(LocalDateTimeConversion.getInstance());
        GenericData.get().addLogicalTypeConversion(BigDecimalConversion.getInstance());
        LogicalTypes.register(LocalDateTimeLogicalType.INSTANCE.getName(), schema -> LocalDateTimeLogicalType.INSTANCE);
        LogicalTypes.register(LocalDateLogicalType.INSTANCE.getName(), schema -> LocalDateLogicalType.INSTANCE);
        LogicalTypes.register(LocalTimeLogicalType.INSTANCE.getName(), schema -> LocalTimeLogicalType.INSTANCE);
        LogicalTypes.register(BigDecimalLogicalType.INSTANCE.getName(), schema -> BigDecimalLogicalType.INSTANCE);
    }

    protected final SpecificData specificData = new SpecificData();

    public SpecificDataConfigurer() {
        specificData.addLogicalTypeConversion(LocalDateConversion.getInstance());
        specificData.addLogicalTypeConversion(LocalTimeConversion.getInstance());
        specificData.addLogicalTypeConversion(LocalDateTimeConversion.getInstance());
        specificData.addLogicalTypeConversion(BigDecimalConversion.getInstance());
    }
}