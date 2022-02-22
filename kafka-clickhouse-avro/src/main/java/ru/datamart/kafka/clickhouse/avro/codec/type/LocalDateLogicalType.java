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
package ru.datamart.kafka.clickhouse.avro.codec.type;

import org.apache.avro.LogicalType;

public class LocalDateLogicalType extends LogicalType {
    public static final LocalDateLogicalType INSTANCE = new LocalDateLogicalType();
    private static final String TYPE_NAME = "date";

    public LocalDateLogicalType() {
        super(TYPE_NAME);
    }

}
