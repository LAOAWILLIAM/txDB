package test.storage.table;

import org.junit.Test;
import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.index.BPlusTreeIndex;
import txDB.storage.page.MetaDataPage;
import txDB.storage.page.Page;
import txDB.storage.page.TablePage;
import txDB.storage.table.*;
import txDB.type.Type;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

public class TableTest {
    // TODO
    String dbName = "test";
    DiskManager diskManager = new DiskManager();
    LockManager lockManager = new LockManager(LockManager.TwoPhaseLockType.REGULAR, LockManager.DeadlockType.DETECTION);
    TransactionManager transactionManager = new TransactionManager(lockManager, null);

    public TableTest() throws IOException {
        diskManager.dropFile(dbName);
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    public void singlePageInsertTupleTest() throws InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        Transaction txn0 = transactionManager.begin();

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        try {
            TablePage tablePage = new TablePage(page0);
            bufferManager.replacePage(tablePage);
            tablePage.initialize(page0.getPageId(), Config.PAGE_SIZE, Config.INVALID_PAGE_ID,
                    null, txn0);
            assertEquals(tablePage.getTablePageId(), 0);

            RecordID recordID = new RecordID(0, 0);
            assertNull(tablePage.getTuple(recordID, txn0, lockManager));

            ArrayList<Column> columns = new ArrayList<>();
            Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
            Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
            Column col2 = new Column("col2", Type.ColumnValueType.INTEGER, 4, 0);
            columns.add(col0);
            columns.add(col1);
            columns.add(col2);
            Scheme scheme = new Scheme(columns);
            ArrayList<Object> values = new ArrayList<>();
            values.add(1);
            values.add(2);
            values.add(3);
            Tuple tuple = new Tuple(values, scheme);
            tablePage.insertTuple(tuple, recordID, txn0, lockManager, null);
            Tuple res = tablePage.getTuple(recordID, txn0, lockManager);
            assertNotNull(res);
            assertEquals(res.getValue(scheme, 0), new Integer(1));
            assertEquals(res.getValue(scheme, 1), new Integer(2));
            assertEquals(res.getValue(scheme, 2), new Integer(3));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockManager.closeDetection();
            diskManager.close();
            diskManager.dropFile(dbName);
        }
    }

    @Test
    public void singlePageFillTupleTest() throws InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        Transaction txn0 = transactionManager.begin();

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        try {
            TablePage tablePage = new TablePage(page0);
            bufferManager.replacePage(tablePage);
            tablePage.initialize(page0.getPageId(), Config.PAGE_SIZE, Config.INVALID_PAGE_ID,
                    null, txn0);
            assertEquals(tablePage.getTablePageId(), 0);

            RecordID recordID = new RecordID(0, 0);
            assertNull(tablePage.getTuple(recordID, txn0, lockManager));

            ArrayList<Column> columns = new ArrayList<>();
            Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
            Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
            Column col2 = new Column("col2", Type.ColumnValueType.INTEGER, 4, 0);
            columns.add(col0);
            columns.add(col1);
            columns.add(col2);
            Scheme scheme = new Scheme(columns);
            ArrayList<Object> values = new ArrayList<>();
            values.add(1);
            values.add(2);
            values.add(3);
            Tuple tuple = new Tuple(values, scheme);

            int i = 0;
            while (tablePage.insertTuple(tuple, recordID, txn0, lockManager, null)) {
                Tuple res = tablePage.getTuple(recordID, txn0, lockManager);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
                i++;
                recordID.setRecordId(0, i);
                values.clear();
                values.add(i * 3 + 1);
                values.add(i * 3 + 2);
                values.add(i * 3 + 3);
                tuple = new Tuple(values, scheme);
            }

            System.out.println("End: page0 full with tuples, tuple count: " + i);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockManager.closeDetection();
            diskManager.close();
            diskManager.dropFile(dbName);
        }
    }

    @Test
    public void tableInsertTupleTest() throws InterruptedException {
        int bufferSize = 3;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        Transaction txn0 = transactionManager.begin();

        try {
            Table table = new Table(bufferManager, lockManager, null, txn0);
            RecordID recordID = new RecordID(0, 0);
            assertNull(table.getTuple(recordID, txn0));

            ArrayList<Column> columns = new ArrayList<>();
            Column col0 = new Column("col0", Type.ColumnValueType.BIGINT, 8, 0);
            Column col1 = new Column("col1", Type.ColumnValueType.BIGINT, 8, 0);
            Column col2 = new Column("col2", Type.ColumnValueType.BIGINT, 8, 0);
            Column col3 = new Column("col3", Type.ColumnValueType.BIGINT, 8, 0);
            columns.add(col0);
            columns.add(col1);
            columns.add(col2);
            columns.add(col3);
            Scheme scheme = new Scheme(columns);
            ArrayList<Object> values = new ArrayList<>();
            Tuple tuple, res;

            int i;
            for (i = 0; i < 700; i++) {
                values.clear();
                values.add((long)(i * 3 + 1));
                values.add((long)(i * 3 + 2));
                values.add((long)(i * 3 + 3));
                values.add((long)(i * 3 + 4));
                tuple = new Tuple(values, scheme);
                assertTrue(table.insertTuple(tuple, recordID, txn0));
                System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex());
                res = table.getTuple(recordID, txn0);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Long(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Long(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Long(i * 3 + 3));
                assertEquals(res.getValue(scheme, 3), new Long(i * 3 + 4));
            }

//            assertFalse(bufferManager.flushPage(0));
//            assertFalse(bufferManager.flushPage(1));
//            assertFalse(bufferManager.flushPage(2));
//            assertFalse(bufferManager.flushPage(3));
//            assertTrue(bufferManager.flushPage(4));
//            assertTrue(bufferManager.flushPage(5));
//            assertTrue(bufferManager.flushPage(6));
            bufferManager.flushAllPages();

            for (i = 0; i < 700; i++) {
                recordID = new RecordID(i / 101, i % 101);
                res = table.getTuple(recordID, txn0);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Long(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Long(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Long(i * 3 + 3));
                assertEquals(res.getValue(scheme, 3), new Long(i * 3 + 4));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockManager.closeDetection();
            diskManager.close();
            diskManager.dropFile(dbName);
        }
    }

    @Test
    public void combineMetaDataAndTableTest() throws InterruptedException {
        int bufferSize = 3;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
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

        Table table = new Table(bufferManager, lockManager, null, txn0);
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
        Tuple tuple, res;
        int i;
        for (i = 0; i < 10000; i++) {
            values.clear();
            values.add(i * 3 + 1);
            values.add(i * 3 + 2);
            values.add(i * 3 + 3);
            tuple = new Tuple(values, scheme);
            assertTrue(table.insertTuple(tuple, recordID, txn0));
//            System.out.println(i + ", " + recordID.getPageId() + ", " + recordID.getTupleIndex());
            res = table.getTuple(recordID, txn0);
            assertNotNull(res);
            assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
            assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
            assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
        }

        bufferManager.flushAllPages();
        assertEquals(bufferManager.getSize(), 0);

        page0 = bufferManager.fetchPage(0);
        assertNotNull(page0);

        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            metaDataPage = (MetaDataPage) in.readObject();
            assertEquals(metaDataPage.getRelationMetaData(relationName).getRootRelationPageId(), 1);
            assertEquals(metaDataPage.getRelationMetaData(relationName).getRelationName(), "table0");

            table = new Table(bufferManager, lockManager, null, metaDataPage.getRelationMetaData(relationName).getRootRelationPageId());
            for (i = 0; i < 10000; i++) {
                recordID = new RecordID((i / 203) + 1, i % 203);
                res = table.getTuple(recordID, txn0);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            lockManager.closeDetection();
            diskManager.close();
//            diskManager.dropFile(dbName);
        }
    }

    /**
     * Require a written file exists
     */
    @Test
    public void tablePersistTest() throws InterruptedException {
        int bufferSize = 3;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        Transaction txn0 = transactionManager.begin();

        Page page0 = bufferManager.fetchPage(0);
        String relationName = "table0";
        RecordID recordID;

        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            MetaDataPage metaDataPage = (MetaDataPage) in.readObject();
            assertEquals(metaDataPage.getRelationMetaData(relationName).getRootRelationPageId(), 1);
            assertEquals(metaDataPage.getRelationMetaData(relationName).getRelationName(), "table0");
            Scheme scheme = metaDataPage.getRelationMetaData(relationName).getScheme();

            Table table = new Table(bufferManager, lockManager, null, metaDataPage.getRelationMetaData(relationName).getRootRelationPageId());
            int i;
            Tuple tuple, res;
            for (i = 0; i < 10000; i++) {
                recordID = new RecordID((i / 203) + 1, i % 203);
                res = table.getTuple(recordID, txn0);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
            }

            bufferManager.flushAllPages();
            assertEquals(bufferManager.getSize(), 0);

            recordID = new RecordID(table.getFirstPageId(), 0);
            ArrayList<Object> values = new ArrayList<>();
            for (i = 10000; i < 20000; i++) {
                values.clear();
                values.add(i * 3 + 1);
                values.add(i * 3 + 2);
                values.add(i * 3 + 3);
                tuple = new Tuple(values, scheme);
                assertTrue(table.insertTuple(tuple, recordID, txn0));
//            System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex());
                res = table.getTuple(recordID, txn0);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
            }
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            lockManager.closeDetection();
            diskManager.close();
//            diskManager.dropFile(dbName);
        }
    }

    @Test
    public void createSecondTableTest() throws InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        Transaction txn0 = transactionManager.begin();

        Page page0 = bufferManager.fetchPage(0);
        String relationName = "table2";

        /**
         * here I do a simulation: create table table1;
         */
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            MetaDataPage metaDataPage = (MetaDataPage) in.readObject();

            ArrayList<Column> columns = new ArrayList<>();
            Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
            Column col1 = new Column("col3", Type.ColumnValueType.INTEGER, 4, 0);
            Column col2 = new Column("col4", Type.ColumnValueType.INTEGER, 4, 0);
            columns.add(col0);
            columns.add(col1);
            columns.add(col2);
            Scheme scheme = new Scheme(columns);

            if (metaDataPage.getRelationMetaData(relationName) == null) {
                Table table = new Table(bufferManager, lockManager, null, txn0);

                MetaDataPage.RelationMetaData relationMetaData =
                        metaDataPage.new RelationMetaData(scheme, relationName, table.getFirstPageId());
                metaDataPage.addRelationMetaData(relationName, relationMetaData);

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
                out.writeObject(metaDataPage);
                page0.setPageData(bos.toByteArray());
                // TODO: meta page size should be considered to avoid overflow
//                System.out.println(bos.toByteArray().length);
                bufferManager.unpinPage(page0.getPageId(), true);
                bufferManager.flushPage(page0.getPageId());
            } else {
                System.out.println(relationName + " already created");
                return;
            }

            Table table = new Table(bufferManager, lockManager, null, metaDataPage.getRelationMetaData(relationName).getRootRelationPageId());
            RecordID recordID = new RecordID(table.getFirstPageId(), 0);

            ArrayList<Object> values = new ArrayList<>();
            Tuple tuple, res;
            int i;
            for (i = 0; i < 1500; i += 15) {
                values.clear();
                values.add(i * 3 + 1);
                values.add(i * 3 + 2);
                values.add(i * 3 + 3);
                tuple = new Tuple(values, scheme);
                assertTrue(table.insertTuple(tuple, recordID, txn0));
//                System.out.println(i + ", " + recordID.getPageId() + ", " + recordID.getTupleIndex());
                res = table.getTuple(recordID, txn0);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
            }

            bufferManager.flushAllPages();
            assertEquals(bufferManager.getSize(), 0);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            lockManager.closeDetection();
            diskManager.close();
        }
    }

    @Test
    public void getTupleWithIndexTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 1000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
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

        Table table = new Table(bufferManager, lockManager, null, txn0);
        assertEquals(table.getFirstPageId(), 1);
        RecordID recordID = new RecordID(table.getFirstPageId(), 0);
        assertNull(table.getTuple(recordID, txn0));

        MetaDataPage.RelationMetaData relationMetaData =
                metaDataPage.new RelationMetaData(scheme, relationName, table.getFirstPageId());
        metaDataPage.addRelationMetaData(relationName, relationMetaData);

        /**
         * here I do a simulation: create index index0 on table0 (col0);
         */
        String indexName = "index0";
        BPlusTreeIndex<Integer, RecordID> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 250, 250);
        assertEquals(bpti.getRootPageId(), 2);
        ArrayList<Column> indexAttributes = new ArrayList<>();
        indexAttributes.add(col0);
        MetaDataPage.IndexMetaData indexMetaData =
                metaDataPage.new IndexMetaData(indexName, relationName, indexAttributes, bpti.getRootPageId());
        metaDataPage.addIndexMetaData(indexName, indexMetaData);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(metaDataPage);
        page0.setPageData(bos.toByteArray());
        bufferManager.unpinPage(page0.getPageId(), true);

        ArrayList<Object> values = new ArrayList<>();
        Tuple tuple, res;
        int i;
        Instant start = Instant.now();
//        RecordID recordID1 = new RecordID(6, 6);
        for (i = 0; i < 10000; i++) {
            values.clear();
            int column0 = i * 3 + 1;
            values.add(column0);
            values.add(column0 + 1);
            values.add(column0 + 2);
            tuple = new Tuple(values, scheme);
//            assertNotNull(table.insertTuple(tuple, txn0));
            bpti.insert(column0, table.insertTuple(tuple, txn0), txn0);
//            bpti.insert(column0, recordID1);
//            assertEquals(bpti.find(column0).getPageId(), 6);
//            System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex());
//            res = table.getTuple(bpti.find(column0, txn0), txn0);
//            assertNotNull(res);
//            assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
//            assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
//            assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
        }
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());

//        for (i = 0; i < 100000; i++) {
////                System.out.println(i);
////            assertEquals(bpti.find(i * 3 + 1).getPageId(), 6);
//            assertEquals(table.getTuple(bpti.find(i * 3 + 1), null).getValue(scheme, 1), new Integer(i * 3 + 2));
//        }

        bufferManager.flushAllPages();
        assertEquals(bufferManager.getSize(), 0);

        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            metaDataPage = (MetaDataPage) in.readObject();
            assertEquals(metaDataPage.getRelationMetaData(relationName).getRootRelationPageId(), 1);
            assertEquals(metaDataPage.getRelationMetaData(relationName).getRelationName(), "table0");
            assertEquals(metaDataPage.getIndexMetaData(indexName).getRootIndexPageId(), 2);
            assertEquals(metaDataPage.getIndexMetaData(indexName).getIndexName(), "index0");

            table = new Table(bufferManager, lockManager, null, metaDataPage.getRelationMetaData(relationName).getRootRelationPageId());
            bpti = new BPlusTreeIndex<>(bufferManager, metaDataPage.getIndexMetaData(indexName).getRootIndexPageId(), 250, 250);
            for (i = 0; i < 10000; i++) {
//                System.out.println(i);
                recordID = bpti.find(i * 3 + 1, txn0);
                assertNotNull(recordID);
                assertEquals(table.getTuple(recordID, txn0).getValue(scheme, 1), new Integer(i * 3 + 2));
            }
//            assertEquals(table.getTuple(bpti.find(3001), null).getValue(scheme, 1), new Integer(3002));
//            assertEquals(table.getTuple(bpti.find(18001), null).getValue(scheme, 1), new Integer(18002));
//            assertEquals(table.getTuple(bpti.find(45001), null).getValue(scheme, 1), new Integer(45002));
//            assertEquals(table.getTuple(bpti.find(450001), null).getValue(scheme, 1), new Integer(450002));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            lockManager.closeDetection();
            diskManager.close();
//            diskManager.dropFile(dbName);
        }
    }

    @Test
    public void varcharTest() throws InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
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
        Column col2 = new Column("col2", Type.ColumnValueType.VARCHAR, 0, 0);
        Column col3 = new Column("col3", Type.ColumnValueType.VARCHAR, 0, 0);
        columns.add(col0);
        columns.add(col1);
        columns.add(col2);
        columns.add(col3);
        Scheme scheme = new Scheme(columns);
        String relationName = "table0";

        Table table = new Table(bufferManager, lockManager, null, txn0);
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
        Tuple tuple, res;
        String val2, val3;
        int i;
        for (i = 0; i < 10000; i++) {
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
//            assertTrue(table.insertTuple(tuple, recordID, txn0));
            recordID = table.insertTuple(tuple, txn0);
            assertNotNull(recordID);
//            System.out.println(i + ", " + recordID.getPageId() + ", " + recordID.getTupleIndex());
            res = table.getTuple(recordID, txn0);
            assertNotNull(res);
            assertEquals(new Integer(i * 3 + 1), res.getValue(scheme, 0));
            assertEquals(new Integer(i * 3 + 2), res.getValue(scheme, 1));
            assertEquals( "hello " + i, res.getValue(scheme, 2));
            assertEquals( "hello world " + i, res.getValue(scheme, 3));
        }

        bufferManager.flushAllPages();
        assertEquals(bufferManager.getSize(), 0);

        page0 = bufferManager.fetchPage(0);
        assertNotNull(page0);

        lockManager.closeDetection();
        diskManager.close();
    }

    @Test
    public void genericsTest() {
        Object res = getGenericsValue();
        assertEquals(res, 2);
        diskManager.close();
    }

    @SuppressWarnings("unchecked")
    public <T> T getGenericsValue() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.putInt(1);
        byteBuffer.putInt(2);
        byteBuffer.putInt(3);
        byteBuffer.putInt(4);
        diskManager.close();
        return (T) (Object)byteBuffer.getInt(4);
    }
}
