package org.reaktivity.nukleus.kafka.internal.stream;

import static org.agrona.BitUtil.align;
import static org.agrona.concurrent.ringbuffer.OneToOneRingBuffer.PADDING_MSG_TYPE_ID;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.ALIGNMENT;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.HEADER_LENGTH;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.messageTypeId;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.recordLength;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.HEAD_POSITION_OFFSET;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.checkCapacity;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TAIL_POSITION_OFFSET;


import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;

public class OneToOneRingBufferSpy
{
    private final int capacity;
    private final AtomicLong headPosition;
    private final AtomicBuffer buffer;


    public OneToOneRingBufferSpy(
            final AtomicBuffer buffer)
    {
        this.buffer = buffer;
        checkCapacity(buffer.capacity());
        capacity = buffer.capacity() - TRAILER_LENGTH;

        buffer.verifyAlignment();

        headPosition = new AtomicLong();
    }

    public void resetHead()
    {
        headPosition.lazySet(buffer.getLong(capacity + HEAD_POSITION_OFFSET));
    }


    public DirectBuffer buffer()
    {
        return buffer;
    }

    public long producerPosition()
    {
        return buffer.getLong(buffer.capacity() - TRAILER_LENGTH + TAIL_POSITION_OFFSET);
    }

    public long consumerPosition()
    {
        return buffer.getLong(buffer.capacity() - TRAILER_LENGTH + HEAD_POSITION_OFFSET);
    }

    public int spy(
            final MessageHandler handler)
    {
        return spy(handler, Integer.MAX_VALUE);
    }

    public int spy(
            final MessageHandler handler,
            final int messageCountLimit)
    {
        int messagesRead = 0;

        final AtomicBuffer buffer = this.buffer;
        final long head = headPosition.get();

        int bytesRead = 0;

        final int capacity = this.capacity;
        final int headIndex = (int)head & (capacity - 1);
        final int contiguousBlockLength = capacity - headIndex;

        try
        {
            while ((bytesRead < contiguousBlockLength) && (messagesRead < messageCountLimit))
            {
                final int recordIndex = headIndex + bytesRead;
                final long header = buffer.getLongVolatile(recordIndex);

                final int recordLength = recordLength(header);
                if (recordLength <= 0)
                {
                    break;
                }

                bytesRead += align(recordLength, ALIGNMENT);

                final int messageTypeId = messageTypeId(header);
                if (PADDING_MSG_TYPE_ID == messageTypeId)
                {
                    continue;
                }

                ++messagesRead;
                handler.onMessage(messageTypeId, buffer, recordIndex + HEADER_LENGTH, recordLength - HEADER_LENGTH);
            }
        }
        finally
        {
            if (bytesRead != 0)
            {
                headPosition.lazySet(head + bytesRead);
            }
        }

        return messagesRead;
    }
}

