package test.storage.table;

import org.junit.Test;
import test.buffer.BufferManagerTest;
import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.recovery.LogManager;
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
import java.util.ArrayList;

public class TableTest {
    // TODO
    @Test
    public void singlePageInsertTupleTest() {
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 100;
        DiskManager diskManager = new DiskManager(dbFilePath);
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        try {
            TablePage tablePage = new TablePage(page0);
            bufferManager.replacePage(tablePage);
            tablePage.initialize(page0.getPageId(), Config.PAGE_SIZE, Config.INVALID_PAGE_ID,
                    null, null);
            assertEquals(tablePage.getTablePageId(), 0);

            RecordID recordID = new RecordID(0, 0);
            assertNull(tablePage.getTuple(recordID, null, null));

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
            tablePage.insertTuple(tuple, recordID, null, null, null);
            Tuple res = tablePage.getTuple(recordID, null, null);
            assertNotNull(res);
            assertEquals(res.getValue(scheme, 0), new Integer(1));
            assertEquals(res.getValue(scheme, 1), new Integer(2));
            assertEquals(res.getValue(scheme, 2), new Integer(3));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            diskManager.close();

            BufferManagerTest.deleteFile(dbFile);
            BufferManagerTest.deleteFile(logFile);
        }
    }

    @Test
    public void singlePageFillTupleTest() {
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 100;
        DiskManager diskManager = new DiskManager(dbFilePath);
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        try {
            TablePage tablePage = new TablePage(page0);
            bufferManager.replacePage(tablePage);
            tablePage.initialize(page0.getPageId(), Config.PAGE_SIZE, Config.INVALID_PAGE_ID,
                    null, null);
            assertEquals(tablePage.getTablePageId(), 0);

            RecordID recordID = new RecordID(0, 0);
            assertNull(tablePage.getTuple(recordID, null, null));

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
            while (tablePage.insertTuple(tuple, recordID, null, null, null)) {
                Tuple res = tablePage.getTuple(recordID, null, null);
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
            diskManager.close();

            BufferManagerTest.deleteFile(dbFile);
            BufferManagerTest.deleteFile(logFile);
        }
    }

    @Test
    public void tableInsertTupleTest() {
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 3;
        DiskManager diskManager = new DiskManager(dbFilePath);
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        try {
            Table table = new Table(bufferManager, null, null, null);
            RecordID recordID = new RecordID(0, 0);
            assertNull(table.getTuple(recordID, null));

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
                assertTrue(table.insertTuple(tuple, recordID, null));
                System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex());
                res = table.getTuple(recordID, null);
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
                res = table.getTuple(recordID, null);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Long(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Long(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Long(i * 3 + 3));
                assertEquals(res.getValue(scheme, 3), new Long(i * 3 + 4));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            diskManager.close();
//            bufferManager.flushAllPages();

            BufferManagerTest.deleteFile(dbFile);
            BufferManagerTest.deleteFile(logFile);
        }
    }

    @Test
    public void combineMetaDataAndTableTest() {
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 3;
        DiskManager diskManager = new DiskManager(dbFilePath);
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

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

        Table table = new Table(bufferManager, null, null, null);
        RecordID recordID = new RecordID(table.getFirstPageId(), 0);
        assertNull(table.getTuple(recordID, null));

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
        for (i = 0; i < 700; i++) {
            values.clear();
            values.add(i * 3 + 1);
            values.add(i * 3 + 2);
            values.add(i * 3 + 3);
            tuple = new Tuple(values, scheme);
            assertTrue(table.insertTuple(tuple, recordID, null));
//            System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex());
            res = table.getTuple(recordID, null);
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

            table = new Table(bufferManager, null, null, metaDataPage.getRelationMetaData(relationName).getRootRelationPageId());
            for (i = 0; i < 700; i++) {
                recordID = new RecordID((i / 203) + 1, i % 203);
                res = table.getTuple(recordID, null);
                assertNotNull(res);
                assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
                assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
                assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            diskManager.close();

            BufferManagerTest.deleteFile(dbFile);
            BufferManagerTest.deleteFile(logFile);
        }
    }

    @Test
    public void getTupleWithIndexTest() throws IOException, ClassNotFoundException {
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 100000;
        DiskManager diskManager = new DiskManager(dbFilePath);
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

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

        Table table = new Table(bufferManager, null, null, null);
        assertEquals(table.getFirstPageId(), 1);
        RecordID recordID = new RecordID(table.getFirstPageId(), 0);
        assertNull(table.getTuple(recordID, null));

        MetaDataPage.RelationMetaData relationMetaData =
                metaDataPage.new RelationMetaData(scheme, relationName, table.getFirstPageId());
        metaDataPage.addRelationMetaData(relationName, relationMetaData);

        /**
         * here I do a simulation: create index index0 on table0 (col0);
         */
        String indexName = "index0";
        BPlusTreeIndex<Integer, RecordID> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 100);
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
//        for (i = 0; i < 1000000; i++) {
//            values.clear();
//            int column0 = i * 3 + 1;
//            values.add(column0);
//            values.add(i * 3 + 2);
//            values.add(i * 3 + 3);
//            tuple = new Tuple(values, scheme);
//            assertTrue(table.insertTuple(tuple, recordID, null));
//            bpti.insert(column0, recordID);
////            System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex());
//            res = table.getTuple(bpti.find(column0), null);
//            assertNotNull(res);
//            assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
//            assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
//            assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
//        }

        int curPageId = table.getFirstPageId();
        for (i = 0; i < 1000000; i++) {
            values.clear();
            int column0 = i * 3 + 1;
            values.add(column0);
            values.add(i * 3 + 2);
            values.add(i * 3 + 3);
            tuple = new Tuple(values, scheme);
            curPageId = table.insertTuple(curPageId, tuple, recordID, null);
            assertNotEquals(-1, curPageId);
            bpti.insert(column0, recordID);
//            System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex());
            res = table.getTuple(bpti.find(column0), null);
            assertNotNull(res);
            assertEquals(res.getValue(scheme, 0), new Integer(i * 3 + 1));
            assertEquals(res.getValue(scheme, 1), new Integer(i * 3 + 2));
            assertEquals(res.getValue(scheme, 2), new Integer(i * 3 + 3));
        }

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

            table = new Table(bufferManager, null, null, metaDataPage.getRelationMetaData(relationName).getRootRelationPageId());
            bpti = new BPlusTreeIndex<>(bufferManager, metaDataPage.getIndexMetaData(indexName).getRootIndexPageId(), 100);
            assertEquals(table.getTuple(bpti.find(3001), null).getValue(scheme, 1), new Integer(3002));
            assertEquals(table.getTuple(bpti.find(18001), null).getValue(scheme, 1), new Integer(18002));
            assertEquals(table.getTuple(bpti.find(450001), null).getValue(scheme, 1), new Integer(450002));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            diskManager.close();

//            BufferManagerTest.deleteFile(dbFile);
//            BufferManagerTest.deleteFile(logFile);
        }
    }

    @Test
    public void genericsTest() {
        Object res = getGenericsValue();
        assertEquals(res, 2);
    }

    @SuppressWarnings("unchecked")
    public <T> T getGenericsValue() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.putInt(1);
        byteBuffer.putInt(2);
        byteBuffer.putInt(3);
        byteBuffer.putInt(4);
        return (T) (Object)byteBuffer.getInt(4);
    }
}
