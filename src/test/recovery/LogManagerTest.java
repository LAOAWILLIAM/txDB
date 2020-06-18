package test.recovery;

import org.junit.Test;
import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.recovery.LogManager;
import txDB.recovery.LogRecord;
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
    public void runtimeLogTest() throws InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        logManager.startPeriodicalFlush();
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
        int i;
        Instant start = Instant.now();
        for (i = 0; i < 25000; i++) {
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

        logManager.closePeriodicalFlush();

        int offset = 0, index = 0;
        byte[] logBytes;
        ByteBuffer logBuffer;
        while ((logBytes = diskManager.readLog(Config.LOG_SIZE, offset)) != null) {
            logBuffer = ByteBuffer.wrap(logBytes);
            while (logBuffer.getInt(index) != 0) {
//                System.out.println(logBuffer.getInt(index + 4) + ", " +
//                        logBuffer.getInt(index + 8) + ", " +
//                        logBuffer.get(index + 12) + ", " +
//                        logBuffer.get(index + 16));
                if (logBuffer.get(index + 16) == 73) {
//                    System.out.println(logBuffer.getInt(index + 24) + ", " +
//                            logBuffer.getInt(index + 28));
                }

                index += logBuffer.getInt(index);
            }
            index = 0;
            offset += Config.LOG_SIZE;
        }

        diskManager.close();
    }
}
