package test.storage.page;

import org.junit.Test;
import test.buffer.BufferManagerTest;
import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.Page;
import txDB.storage.page.MetaDataPage;
import txDB.storage.table.Scheme;
import txDB.storage.table.Column;
import txDB.type.Type.ColumnValueType;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MetaDataPageTest {
    @Test
    public void createTableAndIndexTest() {
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

        /**
         * here I do a simulation: create table table0;
         */
        MetaDataPage metaDataPage = new MetaDataPage();
        ArrayList<Column> columns = new ArrayList<>();
        Column col0 = new Column("col0", ColumnValueType.INTEGER, 4, 0);
        columns.add(col0);
        Scheme scheme = new Scheme(columns);
        String relationName = "table0";
        Page page1 = bufferManager.newPage();
        MetaDataPage.RelationMetaData relationMetaData =
                metaDataPage.new RelationMetaData(scheme, relationName, page1.getPageId());
        metaDataPage.addRelationMetaData(relationName, relationMetaData);

        /**
         * here I do a simulation: create index index0 on table0 (col0);
         */
        String indexName = "index0";
        Page page2 = bufferManager.newPage();
        ArrayList<Column> indexAttributes = new ArrayList<>();
        indexAttributes.add(col0);
        MetaDataPage.IndexMetaData indexMetaData =
                metaDataPage.new IndexMetaData(indexName, relationName, indexAttributes, page2.getPageId());
        metaDataPage.addIndexMetaData(indexName, indexMetaData);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(metaDataPage);
            page0.setPageData(bos.toByteArray());
            bufferManager.unpinPage(page0.getPageId(), true);
            bufferManager.flushPage(page0.getPageId());
        } catch (IOException e) {
            e.printStackTrace();
        }

        page0 = bufferManager.fetchPage(0);

        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            metaDataPage = (MetaDataPage) in.readObject();
            assertEquals(metaDataPage.getRelationMetaData(relationName).rootRelationPageId, 1);
            assertEquals(metaDataPage.getRelationMetaData(relationName).relationName, "table0");
            assertEquals(metaDataPage.getIndexMetaData(indexName).rootIndexPageId, 2);
            assertEquals(metaDataPage.getIndexMetaData(indexName).indexName, "index0");
            assertEquals(metaDataPage.getIndexMetaData(indexName).indexAttributes.get(0).getColumnName(), "col0");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            diskManager.close();

            BufferManagerTest.deleteFile(dbFile);
            BufferManagerTest.deleteFile(logFile);
        }
    }
}
