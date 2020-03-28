package test.storage.table;

import org.junit.Test;
import test.buffer.BufferManagerTest;
import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.Page;
import txDB.storage.page.TablePage;
import txDB.storage.table.Column;
import txDB.storage.table.RecordID;
import txDB.storage.table.Scheme;
import txDB.storage.table.Tuple;
import txDB.type.Type;

import static org.junit.Assert.*;

import java.io.File;
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
//            assertEquals(res.getValue(scheme, 0), new Integer(1));
//            System.out.println(res.getValue(scheme, 1).toString());
//            System.out.println(res.getValue(scheme, 2).toString());
            ByteBuffer byteBuffer = ByteBuffer.wrap(res.getTupleData());
            System.out.println(byteBuffer.getInt());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            diskManager.close();

            BufferManagerTest.deleteFile(dbFile);
            BufferManagerTest.deleteFile(logFile);
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
