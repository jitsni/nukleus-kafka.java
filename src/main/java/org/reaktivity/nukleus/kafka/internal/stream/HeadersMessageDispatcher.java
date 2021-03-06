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
package org.reaktivity.nukleus.kafka.internal.stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;

public class HeadersMessageDispatcher implements MessageDispatcher
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);
    private final Map<DirectBuffer, HeaderValueMessageDispatcher> dispatchersByHeaderKey = new HashMap<>();
    private final List<HeaderValueMessageDispatcher> dispatchers = new ArrayList<HeaderValueMessageDispatcher>();
    private final BroadcastMessageDispatcher broadcast = new BroadcastMessageDispatcher();

    @Override
    public int dispatch(
                 int partition,
                 long requestOffset,
                 long messageOffset,
                 DirectBuffer key,
                 Function<DirectBuffer, DirectBuffer> supplyHeader,
                 long timestamp,
                 DirectBuffer value)
    {
        int result = 0;
        result +=  broadcast.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, value);
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            result += dispatcher.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, value);
        }
        return result;
    }

    @Override
    public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
    {
        broadcast.flush(partition, requestOffset, lastOffset);
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            dispatcher.flush(partition, requestOffset, lastOffset);
        }
    }

    public HeadersMessageDispatcher add(
                ListFW<KafkaHeaderFW> headers,
                int index,
                MessageDispatcher dispatcher)
    {
        final int[] counter = new int[]{0};
        final KafkaHeaderFW header = headers == null ? null : headers.matchFirst(h -> (index == counter[0]++));
        if (header == null)
        {
            broadcast.add(dispatcher);
        }
        else
        {
            String16FW headerKey = header.key();
            int valueOffset = headerKey.offset() + Short.BYTES;
            int valueLength = headerKey.limit() - valueOffset;
            buffer.wrap(headerKey.buffer(), valueOffset, valueLength);
            HeaderValueMessageDispatcher valueDispatcher = dispatchersByHeaderKey.get(buffer);
            if (valueDispatcher == null)
            {
                int bytesLength = headerKey.sizeof() - Short.BYTES;
                UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[bytesLength]);
                keyCopy.putBytes(0, headerKey.buffer(), valueOffset, valueLength);
                valueDispatcher = new HeaderValueMessageDispatcher(keyCopy);
                dispatchersByHeaderKey.put(keyCopy, valueDispatcher);
                dispatchers.add(valueDispatcher);
            }
            valueDispatcher.add(header.value(), headers, index + 1, dispatcher);
        }
        return this;
    }

    public boolean isEmpty()
     {

         return broadcast.isEmpty() && dispatchersByHeaderKey.isEmpty();
     }

    public boolean remove(
            ListFW<KafkaHeaderFW> headers,
            int index,
            MessageDispatcher dispatcher)
    {
        boolean result = false;
        final int[] counter = new int[]{0};
        final KafkaHeaderFW header = headers == null ? null : headers.matchFirst(h -> (index == counter[0]++));
        if (header == null)
        {
            result = broadcast.remove(dispatcher);
        }
        else
        {
            String16FW headerKey = header.key();
            int valueOffset = headerKey.offset() + Short.BYTES;
            int valueLength = headerKey.limit() - valueOffset;
            buffer.wrap(headerKey.buffer(), valueOffset, valueLength);
            HeaderValueMessageDispatcher valueDispatcher = dispatchersByHeaderKey.get(buffer);
            if (valueDispatcher != null)
            {
                result = valueDispatcher.remove(header.value(), headers, index + 1, dispatcher);
                if (valueDispatcher.isEmpty())
                {
                    dispatchersByHeaderKey.remove(buffer);
                    dispatchers.remove(valueDispatcher);
                }
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s, %s)", this.getClass().getSimpleName(), broadcast, toString(dispatchersByHeaderKey));
    }

    private <V> String toString(
        Map<DirectBuffer, V> map)
    {
        StringBuffer result = new StringBuffer(1000);
        result.append('{');
        for (Map.Entry<DirectBuffer, V> entry : map.entrySet())
        {
            result.append('\n');
            result.append(entry.getKey().getStringWithoutLengthUtf8(0, entry.getKey().capacity()));
            result.append(" = ");
            result.append(entry.getValue().toString());
        }
        result.append('}');
        return result.toString();
    }

}

