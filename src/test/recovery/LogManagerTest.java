package test.recovery;

import org.junit.Test;
import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.recovery.CheckpointManager;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.MetaDataPage;
import txDB.storage.page.Page;
import txDB.storage.table.*;
import txDB.type.Type;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class LogManagerTest {
    // TODO
    String dbName = "test";
    DiskManager diskManager = new DiskManager();
    LockManager lockManager = new LockManager(LockManager.TwoPhaseLockType.REGULAR, LockManager.DeadlockType.PREVENTION);
    LogManager logManager = new LogManager(diskManager);
    TransactionManager transactionManager = new TransactionManager(lockManager, logManager);

    public LogManagerTest() throws IOException {
        diskManager.dropFile(dbName);
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    public void runtimeInsertLogTest() {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
        logManager.startFlushService();
        Transaction txn0 = transactionManager.begin();

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        /**
         * here I do a simulation: create table table0;
         */
        MetaDataPage metaDataPage = new MetaDataPage();
        ArrayList<Column> columns = new ArrayList<>();
        Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
        Column col2 = new Column("col2", Type.ColumnValueType.INTEGER, 4, 0);
        columns.add(col0);
        columns.add(col1);
        columns.add(col2);
        Scheme scheme = new Scheme(columns);
        String relationName = "table0";

        Table table = new Table(bufferManager, lockManager, logManager, txn0);
        RecordID recordID = new RecordID(table.getFirstPageId(), 0);
        assertNull(table.getTuple(recordID, txn0));

        MetaDataPage.RelationMetaData relationMetaData =
                metaDataPage.new RelationMetaData(scheme, relationName, table.getFirstPageId());
        metaDataPage.addRelationMetaData(relationName, relationMetaData);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(metaDataPage);
            page0.setPageData(bos.toByteArray());
            bufferManager.unpinPage(page0.getPageId(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList<Object> values = new ArrayList<>();
        Tuple tuple;
        int i, num = 100000;
        Instant start = Instant.now();
        for (i = 0; i < num; i++) {
            values.clear();
            values.add(i * 3 + 1);
            values.add(i * 3 + 2);
            values.add(i * 3 + 3);
            tuple = new Tuple(values, scheme);
            assertTrue(table.insertTuple(tuple, recordID, txn0));
        }

        transactionManager.commit(txn0);

        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());

        bufferManager.flushAllPages();
        assertEquals(bufferManager.getSize(), 0);

        logManager.closeFlushService();

        int offset = 0, index = 0, bufferStart = 0;
        byte[] logBytes;
        ByteBuffer logBuffer;
        boolean whetherBeyond = false;
        i = 0;
        int j = 0;
        while ((logBytes = diskManager.readLog(Config.LOG_SIZE, offset)) != null) {
            logBuffer = ByteBuffer.wrap(logBytes);
            while (logBuffer.getInt(index) != 0) {

                if (index + logBuffer.getInt(index) >= Config.LOG_SIZE) {
                    whetherBeyond = true;
                    bufferStart += index;
                    offset = bufferStart;
                    break;
                }

//                System.out.println(index + ", " +
//                        logBuffer.getInt(index) + ", " +
//                        logBuffer.getInt(index + 4) + ", " +
//                        logBuffer.getInt(index + 8) + ", " +
//                        logBuffer.get(index + 12) + ", " +
//                        logBuffer.get(index + 16));
                if (logBuffer.get(index + 16) == 73) {
//                    System.out.println(logBuffer.getInt(index + 24) + ", " +
//                            logBuffer.getInt(index + 28));

                    // number of commit
                    i++;
                } else if (logBuffer.get(index + 16) == 69) {
                    // number of end
                    j++;
                }

                index += logBuffer.getInt(index);
            }
            index = 0;
            if (!whetherBeyond) {
                offset += Config.LOG_SIZE;
            } else {
                whetherBeyond = false;
            }
        }
        assertEquals(num, i);
        assertEquals(1, j);

        diskManager.close();
    }

    @Test
    public void runtimeUpdateLogTest() {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
        CheckpointManager checkpointManager = new CheckpointManager(bufferManager, transactionManager, logManager);
        logManager.startFlushService();
        checkpointManager.startCheckpointService();
        Transaction txn0 = transactionManager.begin();
        Transaction txn1 = transactionManager.begin();

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        /**
         * here I do a simulation: create table table0;
         */
        MetaDataPage metaDataPage = new MetaDataPage();
        ArrayList<Column> columns = new ArrayList<>();
        Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
        Column col2 = new Column("col2", Type.ColumnValueType.VARCHAR, 0, 0);
        Column col3 = new Column("col3", Type.ColumnValueType.VARCHAR, 0, 0);
        columns.add(col0);
        columns.add(col1);
        columns.add(col2);
        columns.add(col3);
        Scheme scheme = new Scheme(columns);
        String relationName = "table0";

        Table table = new Table(bufferManager, lockManager, logManager, txn0);
        RecordID recordID = new RecordID(table.getFirstPageId(), 0);
        assertNull(table.getTuple(recordID, txn0));

        MetaDataPage.RelationMetaData relationMetaData =
                metaDataPage.new RelationMetaData(scheme, relationName, table.getFirstPageId());
        metaDataPage.addRelationMetaData(relationName, relationMetaData);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(metaDataPage);
            page0.setPageData(bos.toByteArray());
            bufferManager.unpinPage(page0.getPageId(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList<Object> values = new ArrayList<>();
        ArrayList<Integer> unLinedValueLens = new ArrayList<>();
        Tuple tuple;
        String val2, val3;
        int i, num = 10000;
        Instant start = Instant.now();
        for (i = 0; i < num; i++) {
            values.clear();
            values.add(i * 3 + 1);
            values.add(i * 3 + 2);
            val2 = "hello " + i;
            val3 = "hello world " + i;
            values.add(val2);
            values.add(val3);
            unLinedValueLens.clear();
            unLinedValueLens.add(val2.length());
            unLinedValueLens.add(val3.length());
            tuple = new Tuple(values, scheme, unLinedValueLens);
            recordID = table.insertTuple(tuple, txn0);
            assertNotNull(recordID);
            lockManager.unlock(txn0, recordID);

            values.clear();
            values.add(i * 3 + 100);
            values.add(i * 3 + 200);
            val2 = "hello " + (i + 100);
            val3 = "hello world " + (i + 100);
            values.add(val2);
            values.add(val3);
            unLinedValueLens.clear();
            unLinedValueLens.add(val2.length());
            unLinedValueLens.add(val3.length());
            tuple = new Tuple(values, scheme, unLinedValueLens);
            assertTrue(table.updateTuple(tuple, recordID, txn1));
        }

        transactionManager.commit(txn0);
        transactionManager.commit(txn1);

        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());

        bufferManager.flushAllPages();
        assertEquals(bufferManager.getSize(), 0);

        checkpointManager.closeCheckpointService();
        logManager.closeFlushService();

        int offset = 0, index = 0, bufferStart = 0;
        byte[] logBytes;
        ByteBuffer logBuffer;
        boolean whetherBeyond = false;
        Tuple res;
        i = 0;
        int j = 0, k = 0;
        while ((logBytes = diskManager.readLog(Config.LOG_SIZE, offset)) != null) {
            logBuffer = ByteBuffer.wrap(logBytes);
            while (logBuffer.getInt(index) != 0) {

                if (index + logBuffer.getInt(index) >= Config.LOG_SIZE) {
                    whetherBeyond = true;
                    bufferStart += index;
                    offset = bufferStart;
                    break;
                }

//                System.out.println(index + ", " +
//                        logBuffer.getInt(index) + ", " +
//                        logBuffer.getInt(index + 4) + ", " +
//                        logBuffer.getInt(index + 8) + ", " +
//                        logBuffer.get(index + 12) + ", " +
//                        logBuffer.get(index + 16));
                if (logBuffer.get(index + 16) == 73) {
//                    System.out.println(logBuffer.getInt(index + 24) + ", " +
//                            logBuffer.getInt(index + 28));
                } else if (logBuffer.get(index + 16) == 85) {
//                    System.out.println(logBuffer.getInt(index + 24) + ", " +
//                            logBuffer.getInt(index + 28));
                    recordID.setRecordId(logBuffer.getInt(index + 24), logBuffer.getInt(index + 28));
                    int logSize = logBuffer.getInt(index);
                    int oldTupleSize = logBuffer.getInt(index + 32);
                    int newTupleSize = logBuffer.getInt(index + 36 + oldTupleSize);
//                    System.out.println(logSize + ", " + oldTupleSize + ", " + newTupleSize);
                    res = new Tuple(Arrays.copyOfRange(logBytes, index + 40 + oldTupleSize, index + logSize), recordID, newTupleSize, true);
                    assertNotNull(res);
                    assertEquals(new Integer(i * 3 + 100), res.getValue(scheme, 0));
                    assertEquals(new Integer(i * 3 + 200), res.getValue(scheme, 1));
                    assertEquals( "hello " + (i + 100), res.getValue(scheme, 2));
                    assertEquals( "hello world " + (i + 100), res.getValue(scheme, 3));

                    // number of update
                    i++;
                } else if (logBuffer.get(index + 16) == 67) {
                    // number of commit
                    j++;
                } else if (logBuffer.get(index + 16) == 69) {
                    // number of end
                    k++;
                }

                index += logBuffer.getInt(index);
            }
            index = 0;
            if (!whetherBeyond) {
                offset += Config.LOG_SIZE;
            } else {
                whetherBeyond = false;
            }
        }
        assertEquals(num, i);
        assertEquals(2, j);
        assertEquals(2, k);

        diskManager.close();
    }

    @Test
    public void runtimeUpdateAbortTest() {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
        logManager.startFlushService();
        Transaction txn0 = transactionManager.begin();
        Transaction txn1 = transactionManager.begin();

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        /**
         * here I do a simulation: create table table0;
         */
        MetaDataPage metaDataPage = new MetaDataPage();
        ArrayList<Column> columns = new ArrayList<>();
        Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
        Column col2 = new Column("col2", Type.ColumnValueType.VARCHAR, 0, 0);
        Column col3 = new Column("col3", Type.ColumnValueType.VARCHAR, 0, 0);
        columns.add(col0);
        columns.add(col1);
        columns.add(col2);
        columns.add(col3);
        Scheme scheme = new Scheme(columns);
        String relationName = "table0";

        Table table = new Table(bufferManager, lockManager, logManager, txn0);
        RecordID recordID = new RecordID(table.getFirstPageId(), 0);
        assertNull(table.getTuple(recordID, txn0));

        MetaDataPage.RelationMetaData relationMetaData =
                metaDataPage.new RelationMetaData(scheme, relationName, table.getFirstPageId());
        metaDataPage.addRelationMetaData(relationName, relationMetaData);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(metaDataPage);
            page0.setPageData(bos.toByteArray());
            bufferManager.unpinPage(page0.getPageId(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList<Object> values = new ArrayList<>();
        ArrayList<Integer> unLinedValueLens = new ArrayList<>();
        ArrayList<RecordID> recordIDS = new ArrayList<>();
        Tuple tuple;
        String val2, val3;
        int i, num = 100000;
        Instant start = Instant.now();
        for (i = 0; i < num; i++) {
            values.clear();
            values.add(i * 3 + 1);
            values.add(i * 3 + 2);
            val2 = "hello " + i;
            val3 = "hello world " + i;
            values.add(val2);
            values.add(val3);
            unLinedValueLens.clear();
            unLinedValueLens.add(val2.length());
            unLinedValueLens.add(val3.length());
            tuple = new Tuple(values, scheme, unLinedValueLens);
            recordID = table.insertTuple(tuple, txn0);
            // release lock to perform update
            lockManager.unlock(txn0, recordID);

            values.clear();
            values.add(i * 3 + 100);
            values.add(i * 3 + 200);
            val2 = "hello " + (i + 100);
            val3 = "hello world " + (i + 100);
            values.add(val2);
            values.add(val3);
            unLinedValueLens.clear();
            unLinedValueLens.add(val2.length());
            unLinedValueLens.add(val3.length());
            tuple = new Tuple(values, scheme, unLinedValueLens);
            assertTrue(table.updateTuple(tuple, recordID, txn1));
            recordIDS.add(recordID);
        }

        transactionManager.commit(txn0);
        transactionManager.abort(txn1);

        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());

        bufferManager.flushAllPages();
        assertEquals(bufferManager.getSize(), 0);

        Transaction txn2 = transactionManager.begin();
        Tuple res;
        for (i = 0; i < num; i++) {
            res = table.getTuple(recordIDS.get(i), txn2);
            assertNotNull(res);
            assertEquals(new Integer(i * 3 + 1), res.getValue(scheme, 0));
            assertEquals(new Integer(i * 3 + 2), res.getValue(scheme, 1));
            assertEquals( "hello " + i, res.getValue(scheme, 2));
            assertEquals( "hello world " + i, res.getValue(scheme, 3));
        }

        logManager.closeFlushService();

        int offset = 0, index = 0, bufferStart = 0;
        byte[] logBytes;
        ByteBuffer logBuffer;
        boolean whetherBeyond = false;
        i = 0;
        int j = 0, k = 0, q = 0, p = 0;
        while ((logBytes = diskManager.readLog(Config.LOG_SIZE, offset)) != null) {
            logBuffer = ByteBuffer.wrap(logBytes);

            while (logBuffer.getInt(index) != 0) {

                if (index + logBuffer.getInt(index) >= Config.LOG_SIZE) {
                    whetherBeyond = true;
                    bufferStart += index;
                    offset = bufferStart;
                    break;
                }

//                System.out.println(index + ", " +
//                        logBuffer.getInt(index) + ", " +
//                        logBuffer.getInt(index + 4) + ", " +
//                        logBuffer.getInt(index + 8) + ", " +
//                        logBuffer.get(index + 12) + ", " +
//                        logBuffer.get(index + 16));
                if (logBuffer.get(index + 16) == 73) {
//                    System.out.println(logBuffer.getInt(index + 24) + ", " +
//                            logBuffer.getInt(index + 28));
                } else if (logBuffer.get(index + 16) == 85) {
//                    System.out.println(logBuffer.getInt(index + 24) + ", " +
//                            logBuffer.getInt(index + 28));
                    recordID.setRecordId(logBuffer.getInt(index + 24), logBuffer.getInt(index + 28));
                    int logSize = logBuffer.getInt(index);
                    int oldTupleSize = logBuffer.getInt(index + 32);
                    int newTupleSize = logBuffer.getInt(index + 36 + oldTupleSize);
//                    System.out.println(logSize + ", " + oldTupleSize + ", " + newTupleSize);
                    res = new Tuple(Arrays.copyOfRange(logBytes, index + 40 + oldTupleSize, index + logSize), recordID, newTupleSize, true);
                    assertNotNull(res);
                    assertEquals(new Integer(i * 3 + 100), res.getValue(scheme, 0));
                    assertEquals(new Integer(i * 3 + 200), res.getValue(scheme, 1));
                    assertEquals( "hello " + (i + 100), res.getValue(scheme, 2));
                    assertEquals( "hello world " + (i + 100), res.getValue(scheme, 3));

                    // number of update
                    i++;
                } else if (logBuffer.get(index + 17) == 76) {
                    recordID.setRecordId(logBuffer.getInt(index + 24), logBuffer.getInt(index + 28));
                    int logSize = logBuffer.getInt(index);
                    int oldTupleSize = logBuffer.getInt(index + 32);
                    int newTupleSize = logBuffer.getInt(index + 36 + oldTupleSize);
//                    System.out.println(logSize + ", " + oldTupleSize + ", " + newTupleSize);
                    res = new Tuple(Arrays.copyOfRange(logBytes, index + 40 + oldTupleSize, index + logSize - 4), recordID, newTupleSize, true);
                    assertNotNull(res);
                    assertEquals(new Integer((num - j - 1) * 3 + 1), res.getValue(scheme, 0));
                    assertEquals(new Integer((num - j - 1) * 3 + 2), res.getValue(scheme, 1));
                    assertEquals( "hello " + (num - j - 1), res.getValue(scheme, 2));
                    assertEquals( "hello world " + (num - j - 1), res.getValue(scheme, 3));

                    // number of CLR
                    j++;
                } else if (logBuffer.get(index + 17) == 79) {
                    // number of commit
                    k++;
                } else if (logBuffer.get(index + 16) == 65) {
                    // number of abort
                    q++;
                } else if (logBuffer.get(index + 16) == 69) {
                    // number of end
                    p++;
                }

                index += logBuffer.getInt(index);
            }
            index = 0;
            if (!whetherBeyond) {
                offset += Config.LOG_SIZE;
            } else {
                whetherBeyond = false;
            }
        }
        assertEquals(num, i);
        assertEquals(num, j);
        assertEquals(1, k);
        assertEquals(1, q);
        assertEquals(2, p);

        diskManager.close();
    }

    @Test
    public void checkpointTest() {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
        CheckpointManager checkpointManager = new CheckpointManager(bufferManager, transactionManager, logManager);
        logManager.startFlushService();
        checkpointManager.startCheckpointService();
        Transaction txn0 = transactionManager.begin();
        Transaction txn1 = transactionManager.begin();

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        /**
         * here I do a simulation: create table table0;
         */
        MetaDataPage metaDataPage = new MetaDataPage();
        ArrayList<Column> columns = new ArrayList<>();
        Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
        Column col2 = new Column("col2", Type.ColumnValueType.VARCHAR, 0, 0);
        Column col3 = new Column("col3", Type.ColumnValueType.VARCHAR, 0, 0);
        columns.add(col0);
        columns.add(col1);
        columns.add(col2);
        columns.add(col3);
        Scheme scheme = new Scheme(columns);
        String relationName = "table0";

        Table table = new Table(bufferManager, lockManager, logManager, txn0);
        RecordID recordID = new RecordID(table.getFirstPageId(), 0);
        assertNull(table.getTuple(recordID, txn0));

        MetaDataPage.RelationMetaData relationMetaData =
                metaDataPage.new RelationMetaData(scheme, relationName, table.getFirstPageId());
        metaDataPage.addRelationMetaData(relationName, relationMetaData);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(metaDataPage);
            page0.setPageData(bos.toByteArray());
            bufferManager.unpinPage(page0.getPageId(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList<Object> values = new ArrayList<>();
        ArrayList<Integer> unLinedValueLens = new ArrayList<>();
        ArrayList<RecordID> recordIDS = new ArrayList<>();
        Tuple tuple;
        String val2, val3;
        int i, num = 10000;
        Instant start = Instant.now();
        for (i = 0; i < num; i++) {
            values.clear();
            values.add(i * 3 + 1);
            values.add(i * 3 + 2);
            val2 = "hello " + i;
            val3 = "hello world " + i;
            values.add(val2);
            values.add(val3);
            unLinedValueLens.clear();
            unLinedValueLens.add(val2.length());
            unLinedValueLens.add(val3.length());
            tuple = new Tuple(values, scheme, unLinedValueLens);
            recordID = table.insertTuple(tuple, txn0);
            // release lock to perform update
            lockManager.unlock(txn0, recordID);

            values.clear();
            values.add(i * 3 + 100);
            values.add(i * 3 + 200);
            val2 = "hello " + (i + 100);
            val3 = "hello world " + (i + 100);
            values.add(val2);
            values.add(val3);
            unLinedValueLens.clear();
            unLinedValueLens.add(val2.length());
            unLinedValueLens.add(val3.length());
            tuple = new Tuple(values, scheme, unLinedValueLens);
            assertTrue(table.updateTuple(tuple, recordID, txn1));
            recordIDS.add(recordID);
        }

        transactionManager.commit(txn0);
        transactionManager.abort(txn1);

        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());

        bufferManager.flushAllPages();
        assertEquals(bufferManager.getSize(), 0);

        checkpointManager.closeCheckpointService();
        logManager.closeFlushService();

        int offset = 0, index = 0, bufferStart = 0;
        byte[] logBytes;
        ByteBuffer logBuffer;
        boolean whetherBeyond = false;
        i = 0;
        int j = 0, k = 0, q = 0, p = 0;
        Tuple res;
        while ((logBytes = diskManager.readLog(Config.LOG_SIZE, offset)) != null) {
            logBuffer = ByteBuffer.wrap(logBytes);

            while (logBuffer.getInt(index) != 0) {

                if (Config.LOG_SIZE - index <= 4 || index + logBuffer.getInt(index) >= Config.LOG_SIZE) {
                    whetherBeyond = true;
                    bufferStart += index;
                    offset = bufferStart;
                    break;
                }

//                System.out.println(index + ", " +
//                        logBuffer.getInt(index) + ", " +
//                        logBuffer.getInt(index + 4) + ", " +
//                        logBuffer.getInt(index + 8) + ", " +
//                        logBuffer.get(index + 12) + ", " +
//                        logBuffer.get(index + 16));
                if (logBuffer.get(index + 16) == 73) {
//                    System.out.println(logBuffer.getInt(index + 24) + ", " +
//                            logBuffer.getInt(index + 28));
                } else if (logBuffer.get(index + 16) == 85) {
//                    System.out.println(logBuffer.getInt(index + 24) + ", " +
//                            logBuffer.getInt(index + 28));
                    recordID.setRecordId(logBuffer.getInt(index + 24), logBuffer.getInt(index + 28));
                    int logSize = logBuffer.getInt(index);
                    int oldTupleSize = logBuffer.getInt(index + 32);
                    int newTupleSize = logBuffer.getInt(index + 36 + oldTupleSize);
//                    System.out.println(logSize + ", " + oldTupleSize + ", " + newTupleSize);
                    res = new Tuple(Arrays.copyOfRange(logBytes, index + 40 + oldTupleSize, index + logSize), recordID, newTupleSize, true);
                    assertNotNull(res);
                    assertEquals(new Integer(i * 3 + 100), res.getValue(scheme, 0));
                    assertEquals(new Integer(i * 3 + 200), res.getValue(scheme, 1));
                    assertEquals( "hello " + (i + 100), res.getValue(scheme, 2));
                    assertEquals( "hello world " + (i + 100), res.getValue(scheme, 3));

                    // number of update
                    i++;
                } else if (logBuffer.get(index + 17) == 76) {
                    recordID.setRecordId(logBuffer.getInt(index + 24), logBuffer.getInt(index + 28));
                    int logSize = logBuffer.getInt(index);
                    int oldTupleSize = logBuffer.getInt(index + 32);
                    int newTupleSize = logBuffer.getInt(index + 36 + oldTupleSize);
//                    System.out.println(logSize + ", " + oldTupleSize + ", " + newTupleSize);
                    res = new Tuple(Arrays.copyOfRange(logBytes, index + 40 + oldTupleSize, index + logSize), recordID, newTupleSize, true);
                    assertNotNull(res);
                    assertEquals(new Integer((num - j - 1) * 3 + 1), res.getValue(scheme, 0));
                    assertEquals(new Integer((num - j - 1) * 3 + 2), res.getValue(scheme, 1));
                    assertEquals( "hello " + (num - j - 1), res.getValue(scheme, 2));
                    assertEquals( "hello world " + (num - j - 1), res.getValue(scheme, 3));

                    // number of CLR
                    j++;
                } else if (logBuffer.get(index + 17) == 79) {
                    // number of commit
                    k++;
                } else if (logBuffer.get(index + 16) == 65) {
                    // number of abort
                    q++;
                } else if (logBuffer.get(index + 16) == 69) {
                    // number of end
                    p++;
                } else if (logBuffer.get(index + 12) == 67) {
                    Page page = bufferManager.fetchPage(0);
                    ByteArrayInputStream bis = new ByteArrayInputStream(page.getPageData());
                    try {
                        ObjectInputStream in = new ObjectInputStream(bis);
                        metaDataPage = (MetaDataPage) in.readObject();
                        System.out.println("last checkpoint offset: " + metaDataPage.getLastCheckpointOffset());
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }

                index += logBuffer.getInt(index);
            }
            index = 0;
            if (!whetherBeyond) {
                offset += Config.LOG_SIZE;
            } else {
                whetherBeyond = false;
            }
        }
        assertEquals(num, i);
        assertEquals(num, j);
        assertEquals(1, k);
        assertEquals(1, q);
        assertEquals(2, p);

        diskManager.close();
    }
}
