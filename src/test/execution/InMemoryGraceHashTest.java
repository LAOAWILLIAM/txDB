package test.execution;

import txDB.execution.hash.InMemoryGraceHash;
import txDB.storage.table.RecordID;

import org.junit.Test;
import static org.junit.Assert.*;

public class InMemoryGraceHashTest {
    @Test
    public void insertAndFindTest() {
        int bucketSize = 1000;
        InMemoryGraceHash<Integer, RecordID> inMemoryGraceHash = new InMemoryGraceHash<>(bucketSize);

        int i;
        RecordID recordID = new RecordID(0, 0);
        for (i = 0; i < 1000000; i++) {
            recordID.setRecordId(i % bucketSize, 0);
            inMemoryGraceHash.insert(i, recordID);
        }

        for (i = 0; i < 1000000; i++) {
            recordID.setRecordId(i % bucketSize, 0);
            assertEquals(recordID, inMemoryGraceHash.find(i));
        }
    }

    @Test
    public void deleteTest() {
        // TODO
    }
}
