/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.WriteBatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RocksDBConsumeQueueRangeQueryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ConsumeQueueRocksDBStorage storage;
    private RocksDBConsumeQueueTable table;

    @Before
    public void setUp() throws Exception {
        DefaultMessageStore messageStore = mock(DefaultMessageStore.class);
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        storage = new ConsumeQueueRocksDBStorage(messageStore, temporaryFolder.newFolder("cq").getPath());
        assertTrue(storage.start());
        table = new RocksDBConsumeQueueTable(storage, messageStore);
        table.load();
    }

    @After
    public void tearDown() {
        if (storage != null) {
            storage.shutdown();
        }
    }

    @Test
    public void readsOnlyTheRequestedContinuousOffsetsTest() throws Exception {
        for (int offset = 0; offset < 8; offset++) {
            put("topic", 0, offset);
        }
        List<ByteBuffer> rows = table.rangeQuery("topic", 0, 2, 3);
        assertRows(rows, 2, 3, 4);
        assertRows(table.rangeQuery("topic", 0, 7, 10), 7);
    }

    @Test
    public void stopsAtTheFirstMissingOffsetTest() throws Exception {
        put("topic", 0, 5);
        put("topic", 0, 7);
        put("topic", 0, 8);
        assertRows(table.rangeQuery("topic", 0, 5, 4), 5);
    }

    @Test
    public void returnsEmptyWhenTheStartingOffsetIsMissingTest() throws Exception {
        put("topic", 0, 6);
        assertTrue(table.rangeQuery("topic", 0, 5, 4).isEmpty());
    }

    @Test
    public void doesNotReadTheNextQueueOrTopicTest() throws Exception {
        put("aaa", 0, 5);
        put("aaa", 1, 0);
        put("bbb", 0, 0);
        assertRows(table.rangeQuery("aaa", 0, 5, 20), 5);
        assertTrue(table.rangeQuery("aaa", 2, 0, 20).isEmpty());
    }

    @Test
    public void returnsEmptyForAnEmptyQueueOrZeroCountTest() throws Exception {
        assertTrue(table.rangeQuery("missing", 0, 0, 10).isEmpty());
        put("topic", 0, 0);
        assertTrue(table.rangeQuery("topic", 0, 0, 0).isEmpty());
    }

    @Test
    public void readsASingleOffsetWithoutSkippingMissingRowsTest() throws Exception {
        put("topic", 0, 6);
        assertRows(table.rangeQuery("topic", 0, 6, 1), 6);
        assertTrue(table.rangeQuery("topic", 0, 5, 1).isEmpty());
    }

    @Test
    public void readsOffsetsAtTheLongBoundaryTest() throws Exception {
        put("topic", 0, Long.MAX_VALUE - 1);
        put("topic", 0, Long.MAX_VALUE);
        assertRows(table.rangeQuery("topic", 0, Long.MAX_VALUE - 1, 2),
            Long.MAX_VALUE - 1, Long.MAX_VALUE);
    }

    @Test
    public void preservesRangeResultsAfterFlushingToDiskTest() throws Exception {
        put("topic", 0, 1);
        put("topic", 0, 2);
        put("topic", 0, 4);
        storage.flushAll();
        assertRows(table.rangeQuery("topic", 0, 1, 5), 1, 2);
        assertRows(table.rangeQuery("topic", 0, 4, 2), 4);
    }

    @Test
    public void rejectsReadsAfterStorageShutdownTest() {
        storage.shutdown();
        assertThrows(IllegalStateException.class, () -> table.rangeQuery("topic", 0, 0, 10));
    }

    private void put(String topic, int queueId, long offset) throws Exception {
        DispatchEntry entry = new DispatchEntry();
        entry.topic = topic.getBytes(StandardCharsets.UTF_8);
        entry.queueId = queueId;
        entry.queueOffset = offset;
        entry.commitLogOffset = offset;
        entry.messageSize = 100;
        entry.tagCode = 42;
        entry.storeTimestamp = 1234;
        Pair<ByteBuffer, ByteBuffer> buffers = RocksDBConsumeQueueTable.getCQByteBufferPair();
        try (WriteBatch batch = new WriteBatch()) {
            table.buildAndPutCQByteBuffer(buffers, entry, batch);
            storage.batchPut(batch);
        }
    }

    private void assertRows(List<ByteBuffer> rows, long... offsets) {
        assertEquals(offsets.length, rows.size());
        for (int index = 0; index < offsets.length; index++) {
            ByteBuffer row = rows.get(index);
            assertEquals(RocksDBConsumeQueueTable.CQ_UNIT_SIZE, row.remaining());
            assertEquals(offsets[index], row.getLong());
            assertEquals(100, row.getInt());
            assertEquals(42, row.getLong());
            assertEquals(1234, row.getLong());
        }
    }
}
