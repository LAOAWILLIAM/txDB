package test.execution;

import org.junit.Test;
import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.TransactionManager;
import txDB.execution.hash.GraceHash;
import txDB.storage.disk.DiskManager;
import txDB.storage.table.RecordID;

import java.io.IOException;

public class GraceHashTest {
    // TODO
    String dbName = "hash-test";
    DiskManager diskManager = new DiskManager();
    LockManager lockManager = new LockManager(LockManager.TwoPhaseLockType.REGULAR, LockManager.DeadlockType.DETECTION);
    TransactionManager transactionManager = new TransactionManager(lockManager, null);

    public GraceHashTest() throws IOException {
        diskManager.dropFile(dbName);
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    public void graceHashInsertTest() {
        int bufferSize = 1000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        GraceHash<Integer, RecordID> graceHash = new GraceHash<>(500, bufferManager);

        int i;
        RecordID recordID = new RecordID(0, 0);
        for (i = 0; i < 10000; i++) {
            recordID.setRecordId(i % 1000, 0);
            graceHash.insert(i, recordID);
        }

        bufferManager.flushAllPages();
        diskManager.close();
        lockManager.closeDetection();
    }

    @Test
    public void graceHashDeleteTest() {

    }
}
