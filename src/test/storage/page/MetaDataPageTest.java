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
    String dbName = "test";
    DiskManager diskManager = new DiskManager();

    public MetaDataPageTest() throws IOException {
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    public void createTableAndIndexTest() {
        int bufferSize = 100;
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
            assertEquals(metaDataPage.getRelationMetaData(relationName).getRootRelationPageId(), 1);
            assertEquals(metaDataPage.getRelationMetaData(relationName).getRelationName(), "table0");
            assertEquals(metaDataPage.getIndexMetaData(indexName).getRootIndexPageId(), 2);
            assertEquals(metaDataPage.getIndexMetaData(indexName).getIndexName(), "index0");
            assertEquals(metaDataPage.getIndexMetaData(indexName).getIndexAttributes().get(0).getColumnName(), "col0");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            diskManager.close();

            diskManager.dropFile(dbName);
        }
    }
}
