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
package ru.datamart.kafka.clickhouse.avro.codec.conversion;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocalDateConversionTest {

    LocalDateConversion localDateConversion = LocalDateConversion.getInstance();

    private final LocalDate FIRST_DAY_OF_EPOCH = LocalDate.of(1970, 1, 1);

    @Test
    public void daysToLocalDate() {
        LocalDate date = localDateConversion.fromLong(0L, null, null);
        assertEquals(date, FIRST_DAY_OF_EPOCH);
    }

    @Test
    public void localDateToDays() {
        Long day = localDateConversion.toLong(FIRST_DAY_OF_EPOCH, null, null);
        assertEquals(0L, day);
    }

}
