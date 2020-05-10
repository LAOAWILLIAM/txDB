package test.concurrency;

import org.junit.Test;
import txDB.concurrency.LockManager;
import txDB.concurrency.LockManager.*;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.storage.table.RecordID;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class LockManagerTest {
    @Test
    public void deadlockDetectionTest() throws Exception {
        LockManager lockManager = new LockManager(twoPhaseLockType.REGULAR, deadlockType.DETECTION);
        TransactionManager transactionManager = new TransactionManager(lockManager, null);

        RecordID recordID = new RecordID(0, 1);

        Transaction txn1 = transactionManager.begin();
        Transaction txn2 = transactionManager.begin();
        Transaction txn3 = transactionManager.begin();
        Transaction txn4 = transactionManager.begin();

        class SharedOperation implements Runnable {
            private Transaction txn;
            private RecordID recordID;
            public SharedOperation(Transaction txn, RecordID recordID) {
                this.txn = txn;
                this.recordID = recordID;
            }

            @Override
            public void run() {
                try {
                    lockManager.acquireSharedLock(txn, recordID);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        class ExclusiveOperation implements Runnable {
            private Transaction txn;
            private RecordID recordID;
            public ExclusiveOperation(Transaction txn, RecordID recordID) {
                this.txn = txn;
                this.recordID = recordID;
            }

            @Override
            public void run() {
                try {
                    lockManager.acquireExclusiveLock(txn, recordID);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new SharedOperation(txn1, recordID));
        executorService.execute(new SharedOperation(txn2, recordID));
        executorService.execute(new SharedOperation(txn3, recordID));
        executorService.execute(new ExclusiveOperation(txn4, recordID));

        TimeUnit.SECONDS.sleep(5);
        executorService.shutdown();
    }
}
