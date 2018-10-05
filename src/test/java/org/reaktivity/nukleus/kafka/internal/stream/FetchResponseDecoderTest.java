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

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.function.StringIntLongToLongFunction;
import org.reaktivity.nukleus.kafka.internal.function.StringIntToLongFunction;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.function.Function;

import static org.agrona.IoUtil.createEmptyFile;
import static org.agrona.IoUtil.mapExistingFile;

public class FetchResponseDecoderTest
{
    @Test
    public void shouldAddDispatcherWithEmptyHeadersAndNullKey()
    {
        // set up decoder
        MutableDirectBuffer slot = new UnsafeBuffer(new byte[1000000]);
        Function<String, DecoderMessageDispatcher> dispatcher = (topic) -> new DecoderMessageDispatcher() {
            @Override
            public void startOffset(int partition, long lowWatermark) {

            }

            @Override
            public int dispatch(int partition, long requestOffset, long messageOffset, long highWatermark, DirectBuffer key, HeadersFW headers, long timestamp, long traceId, DirectBuffer value) {
                return 0;
            }

            @Override
            public void flush(int partition, long requestOffset, long lastOffset) {

            }
        };
        StringIntToLongFunction requestedOffsetForPartition = (s, i) -> 0L;
        StringIntLongToLongFunction updateStartOffsetForPartition = (s, i, l) -> 0L;

        FetchResponseDecoder decoder = new FetchResponseDecoder(dispatcher, requestedOffsetForPartition, updateStartOffsetForPartition, null, slot);

        // set up ring buffer
        Path path = Paths.get("/Users/jitu/issues/864/oct2/tcp");
        final long streamsCapacity = 0x2000000L;
        final long streamId = 0x2fc7aL;
        final int offset = 0x1ea65c0;

        final File streams = path.toFile();
        final long streamsSize = streamsCapacity + RingBufferDescriptor.TRAILER_LENGTH;
        final MappedByteBuffer mappedStreams = mapExistingFile(streams, "streams", 0, streamsSize);
        final AtomicBuffer atomicStreams = new UnsafeBuffer(mappedStreams);
        OneToOneRingBufferSpy spy = new OneToOneRingBufferSpy(atomicStreams);

        spy.spy(new MessageHandler() {
            boolean start = false;
            @Override
            public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length) {
                if (msgTypeId == DataFW.TYPE_ID) {
                    DataFW data = new DataFW();
                    data.wrap(buffer, index, index + length);
                    System.out.printf("DATA  streamid=%x index = %x length = %d\n", data.streamId(), index, length);
                    if (!start && index == 0x1ea6598) {
                        start = true;
                    }
                    if (data.streamId() == streamId && start) {
                        //System.out.printf("stream index = %x length = %d\n", index, length);
                        int newOffset = decoder.decode(data.payload(), data.trace());
                        System.out.printf("newOffset = %x\n", newOffset);
                    }
                }
            }
        });


    }

}
