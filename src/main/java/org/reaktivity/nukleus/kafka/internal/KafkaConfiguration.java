/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.kafka.internal;

import org.reaktivity.nukleus.Configuration;

public class KafkaConfiguration extends Configuration
{
    public static final String FETCH_MAX_BYTES_PROPERTY = "nukleus.kafka.fetch.max.bytes";

    public static final int FETCH_MAX_BYTES_DEFAULT = 50 * 1024 * 1024;

    public KafkaConfiguration(
        Configuration config)
    {
        super(config);
    }

    public int fetchMaxBytes()
    {
        return getInteger(FETCH_MAX_BYTES_PROPERTY, FETCH_MAX_BYTES_DEFAULT);
    }
}
