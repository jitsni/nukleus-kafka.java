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
package org.reaktivity.nukleus.kafka.internal.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.function.LongSupplier;

import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;

public class DefaultMemoryManagerTest
{
    private UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[1]);
    private static final int MB_128 = 128 * 1024 * 1024;
    private static final int KB = 1024;
    private static final int BYTES_64 = 64;
    private static final int BYTES_128 = 128;

    @Rule
    public DefaultMemoryManagerRule memoryManagerRule = new DefaultMemoryManagerRule();

    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldAllocateAndReleaseLargestBlock()
    {
        memoryManagerRule.assertReleased();
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();

        long addressOffset = memoryManager.acquire(KB);
        assertEquals(0, addressOffset);
        long resolvedOffset = memoryManager.resolve(addressOffset);
        writeBuffer.wrap(resolvedOffset, KB);
        long expected = 0xffffffffffffffffL;
        writeBuffer.putLong(0, expected);
        long actual = writeBuffer.getLong(0);
        assertEquals(expected, actual);

        assertEquals(-1, memoryManager.acquire(KB));
        assertEquals(-1, memoryManager.acquire(KB / 2));

        memoryManagerRule.assertNotReleased();

        memoryManager.release(addressOffset, KB);
        memoryManagerRule.assertReleased();

        addressOffset = memoryManager.acquire(KB);
        resolvedOffset = memoryManager.resolve(addressOffset);
        writeBuffer.wrap(resolvedOffset, KB);
        expected = 0xffffffffffffffffL;
        writeBuffer.putLong(0, expected);
        actual = writeBuffer.getLong(0);
        assertEquals(expected, actual);
    }

    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldAllocateAndReleaseSmallestBlocks()
    {
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();

        for (int allocateAndReleased = 2; allocateAndReleased != 0; allocateAndReleased--)
        {
            LongArrayList acquiredAddresses = new LongArrayList();
            for (int i = 0; (i * BYTES_64) < KB; i++)
            {
                long addressOffset = memoryManager.acquire(BYTES_64);
                long resolvedOffset = memoryManager.resolve(addressOffset);
                assertEquals(i * BYTES_64, addressOffset);
                writeBuffer.wrap(resolvedOffset, BYTES_64);
                writeBuffer.putLong(0, i % BYTES_64);
                acquiredAddresses.add(addressOffset);
                memoryManagerRule.assertNotReleased();
            }
            assertEquals(KB/BYTES_64, acquiredAddresses.size());
            for (int i = 0; (i * BYTES_64) < KB; i++)
            {
                long addressOffset = acquiredAddresses.get(i);
                long resolvedOffset = memoryManager.resolve(addressOffset);
                writeBuffer.wrap(resolvedOffset, BYTES_64);
                assertEquals(i % BYTES_64, writeBuffer.getLong(0));
            }
            for (int i = 0; (i * BYTES_64) < KB; i++)
            {
                long addressOffset = acquiredAddresses.get(i);
                memoryManager.release(addressOffset, BYTES_64);
            }
            memoryManagerRule.assertReleased();
        }
    }

    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldAllocateAndReleaseMediumBlocks()
    {
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();

        for (int allocateAndReleased = 2; allocateAndReleased != 0; allocateAndReleased--)
        {
            LongArrayList acquiredAddresses = new LongArrayList();
            for (int i = 0; (i * BYTES_128) < KB; i++)
            {
                long addressOffset = memoryManager.acquire(BYTES_128);
                long resolvedOffset = memoryManager.resolve(addressOffset);
                assertEquals(i * BYTES_128, addressOffset);
                writeBuffer.wrap(resolvedOffset, BYTES_128);
                writeBuffer.putLong(0, i % BYTES_128);
                acquiredAddresses.add(addressOffset);
                memoryManagerRule.assertNotReleased();
            }
            assertEquals(KB/BYTES_128, acquiredAddresses.size());
            for (int i = 0; (i * BYTES_128) < KB; i++)
            {
                long addressOffset = acquiredAddresses.get(i);
                long resolvedOffset = memoryManager.resolve(addressOffset);
                writeBuffer.wrap(resolvedOffset, BYTES_128);
                assertEquals(i % BYTES_128, writeBuffer.getLong(0));
            }
            for (int i = 0; (i * BYTES_128) < KB; i++)
            {
                long addressOffset = acquiredAddresses.get(i);
                memoryManager.release(addressOffset, BYTES_128);
            }
            memoryManagerRule.assertReleased();
        }
    }

    @Test
    @ConfigureMemoryLayout(capacity = MB_128, smallestBlockSize = KB)
    public void shouldAllocateAndReleaseMediumBlocksLargeCache()
    {
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();
        memoryManagerRule.assertReleased();


        long address1 = memoryManager.acquire(4238);
        long address2 = memoryManager.acquire(3024);
        long address3 = memoryManager.acquire(13743);

        memoryManager.release(address3, 13743);
        memoryManager.release(address2, 3024);
        memoryManager.release(address1, 4238);

        assertTrue(address2 - address1 > 4238);
        assertTrue(address3 - address2 > 3024);
        memoryManagerRule.assertReleased();
    }

    @Test
    @ConfigureMemoryLayout(capacity = MB_128, smallestBlockSize = KB)
    public void shouldNotAcquireAddressZeroWhenNotReleased()
    {
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();
        memoryManagerRule.assertReleased();

        long address0 = memoryManager.acquire(6498);
        assertEquals(0, address0);
        long address1 = memoryManager.acquire(13743);
        assertEquals(16384, address1);

        long address = 0;
        while (address != -1L)
        {
            address = memoryManager.acquire(140);
        }

        memoryManager.release(address0, 6498);

        address = memoryManager.acquire(17000);
        assertEquals(-1L, address);

        address = memoryManager.acquire(8192);
        assertEquals(0L, address);

        address = memoryManager.acquire(140);
        assertEquals(-1L, address);
    }

    @Test
    @ConfigureMemoryLayout(capacity = KB, smallestBlockSize = BYTES_64)
    public void shouldAllocateAndReleaseMixedSizeBlocks()
    {
        final MemoryManager memoryManager = memoryManagerRule.memoryManager();
        final LongArrayList acquired128Blocks = new LongArrayList();
        final LongArrayList acquired64Blocks = new LongArrayList();

        LongSupplier acquire128Address = () ->
        {
            long addressOffset = memoryManager.acquire(BYTES_128);
            if (addressOffset != -1)
            {
                acquired128Blocks.add(addressOffset);
            }
            return addressOffset;
        };

        LongSupplier acquire64Address = () ->
        {
            long addressOffset = memoryManager.acquire(BYTES_64);
            if (addressOffset != -1)
            {
                acquired64Blocks.add(addressOffset);
            }
            return addressOffset;
        };

        for (; acquired128Blocks.size() + acquired64Blocks.size() < 12; )
        {
            assertNotEquals(-1, acquire128Address.getAsLong());
            assertNotEquals(-1, acquire64Address.getAsLong());
            assertNotEquals(-1, acquire64Address.getAsLong());
            acquired128Blocks.forEach(l ->
            {
                assertFalse(acquired64Blocks.contains(l));
            });
        }
        assertEquals(-1, acquire128Address.getAsLong());
        assertEquals(-1, acquire64Address.getAsLong());

        memoryManager.release(acquired128Blocks.remove(3), BYTES_128);
        assertNotEquals(-1, acquire64Address.getAsLong());
        assertNotEquals(-1, acquire64Address.getAsLong());
        assertEquals(-1, acquire128Address.getAsLong());
        assertEquals(-1, acquire64Address.getAsLong());

        // Freeing 64 byte nodes not under same parent does not allow 128 allocation
        memoryManager.release(acquired64Blocks.remove(3), BYTES_64);
        memoryManager.release(acquired64Blocks.remove(3), BYTES_64);
        assertEquals(-1, acquire128Address.getAsLong());

        // Freeing 64 node whose sibbling is now free does allow 128 allocation
        memoryManager.release(acquired64Blocks.remove(3), BYTES_64);
        assertNotEquals(-1, acquire128Address.getAsLong());

        assertEquals(-1, acquire128Address.getAsLong());
        assertNotEquals(-1, acquire64Address.getAsLong());
        assertEquals(-1, acquire64Address.getAsLong());
    }
}