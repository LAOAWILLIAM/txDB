package test.concurrency;

import org.junit.Test;
import txDB.concurrency.LockManager;
import txDB.concurrency.LockManager.*;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.storage.table.RecordID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class LockManagerTest {
    LockManager lockManager = new LockManager(twoPhaseLockType.REGULAR, deadlockType.DETECTION);
    TransactionManager transactionManager = new TransactionManager(lockManager, null);

    @Test
    public void sharedAndExclusiveLockTest() throws Exception {
        class SharedOperation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            private int count = 5;
            public SharedOperation(Transaction txn, RecordID recordID) {
                this.txn = txn;
                this.recordID = recordID;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
//                    while (count-- >= 0) {
                    res = lockManager.acquireSharedLock(txn, recordID);
//                        TimeUnit.SECONDS.sleep(1);
                    lockManager.unlock(txn, recordID);
//                Thread.yield();
//                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        class ExclusiveOperation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            public ExclusiveOperation(Transaction txn, RecordID recordID) {
                this.txn = txn;
                this.recordID = recordID;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    res = lockManager.acquireExclusiveLock(txn, recordID);
//                    TimeUnit.SECONDS.sleep(1);
                    lockManager.unlock(txn, recordID);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        RecordID recordID0 = new RecordID(0, 0);
        RecordID recordID1 = new RecordID(0, 1);

        Transaction txn0 = transactionManager.begin();
        Transaction txn1 = transactionManager.begin();
        Transaction txn2 = transactionManager.begin();
        Transaction txn3 = transactionManager.begin();
        Transaction txn4 = transactionManager.begin();
        Transaction txn5 = transactionManager.begin();

        ExecutorService executorService = Executors.newCachedThreadPool();
        assertTrue(executorService.submit(new SharedOperation(txn5, recordID1)).get());
        assertTrue(executorService.submit(new ExclusiveOperation(txn3, recordID0)).get());
        assertTrue(executorService.submit(new SharedOperation(txn0, recordID0)).get());
        assertTrue(executorService.submit(new ExclusiveOperation(txn4, recordID0)).get());
        assertTrue(executorService.submit(new SharedOperation(txn1, recordID0)).get());
        assertTrue(executorService.submit(new SharedOperation(txn2, recordID0)).get());

//        TimeUnit.SECONDS.sleep(3);
        executorService.shutdown();
        lockManager.close();
    }

    @Test
    public void deadlockDetectionTest() throws Exception {
        class SharedOperation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            public SharedOperation(Transaction txn, RecordID recordID) {
                this.txn = txn;
                this.recordID = recordID;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    res = lockManager.acquireSharedLock(txn, recordID);
//                    TimeUnit.SECONDS.sleep(1);
//                    lockManager.unlock(txn, recordID);
//                    Thread.yield();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        class ExclusiveOperation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            public ExclusiveOperation(Transaction txn, RecordID recordID) {
                this.txn = txn;
                this.recordID = recordID;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    res = lockManager.acquireExclusiveLock(txn, recordID);
//                    TimeUnit.SECONDS.sleep(1);
//                    lockManager.unlock(txn, recordID);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        RecordID recordID0 = new RecordID(0, 0);
        RecordID recordID1 = new RecordID(0, 1);
        RecordID recordID2 = new RecordID(0, 2);

        Transaction txn0 = transactionManager.begin();
        Transaction txn1 = transactionManager.begin();
        Transaction txn2 = transactionManager.begin();

        ExecutorService executorService = Executors.newCachedThreadPool();
        TimeUnit.SECONDS.sleep(2);
        executorService.submit(new SharedOperation(txn0, recordID0));
        TimeUnit.MILLISECONDS.sleep(100);
        executorService.submit(new ExclusiveOperation(txn1, recordID1));
        TimeUnit.MILLISECONDS.sleep(100);
        executorService.submit(new SharedOperation(txn2, recordID2));
        TimeUnit.MILLISECONDS.sleep(100);
        executorService.submit(new SharedOperation(txn0, recordID1));
        TimeUnit.MILLISECONDS.sleep(100);
        executorService.submit(new ExclusiveOperation(txn1, recordID2));
        TimeUnit.MILLISECONDS.sleep(100);
        executorService.submit(new ExclusiveOperation(txn2, recordID0));

        TimeUnit.SECONDS.sleep(2);
        int[] expectArray = {2, 1, 0, 2};
        Stack<Integer> actualStack = lockManager.getCycles().get(0);
        for (int i = 0; i < actualStack.size(); i++) {
            assertEquals(actualStack.get(i), new Integer(expectArray[i]));
        }
        executorService.shutdown();
        lockManager.close();
    }
}
