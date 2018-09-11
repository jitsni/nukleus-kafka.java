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
package org.reaktivity.nukleus.kafka.internal.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.kafka.internal.KafkaController;
import org.reaktivity.reaktor.test.ReaktorRule;

public class KafkaCountersRule implements TestRule
{
    private final ReaktorRule reaktor;

    public KafkaCountersRule(ReaktorRule reaktor)
    {
        this.reaktor = reaktor;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                base.evaluate();
            }
        };
    }

    public long cacheHits()
    {
        return controller().count("cache.hits");
    }

    public long cacheMisses()
    {
        return controller().count("cache.misses");
    }

    public long cacheInuse()
    {
        return controller().count("cache.inuse");
    }

    private KafkaController controller()
    {
        return reaktor.controller(KafkaController.class);
    }

}