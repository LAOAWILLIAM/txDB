package test.concurrency;

import org.junit.Test;
import txDB.concurrency.LockManager;
import txDB.concurrency.LockManager.*;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.storage.table.RecordID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class LockManagerTest {
    LockManager lockManager = new LockManager(twoPhaseLockType.REGULAR, deadlockType.DETECTION);
    TransactionManager transactionManager = new TransactionManager(lockManager, null);
    Random rand = new Random();
    ExecutorService executorService = Executors.newCachedThreadPool();

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
                    TimeUnit.MILLISECONDS.sleep(100 + rand.nextInt(400));
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
                    TimeUnit.MILLISECONDS.sleep(100 + rand.nextInt(400));
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

//        assertTrue(executorService.submit(new SharedOperation(txn5, recordID1)).get());
//        assertTrue(executorService.submit(new ExclusiveOperation(txn3, recordID0)).get());
//        assertTrue(executorService.submit(new SharedOperation(txn0, recordID0)).get());
//        assertTrue(executorService.submit(new ExclusiveOperation(txn4, recordID0)).get());
//        assertTrue(executorService.submit(new SharedOperation(txn1, recordID0)).get());
//        assertTrue(executorService.submit(new SharedOperation(txn2, recordID0)).get());
        executorService.submit(new SharedOperation(txn5, recordID1));
        executorService.submit(new ExclusiveOperation(txn3, recordID0));
        executorService.submit(new SharedOperation(txn0, recordID0));
        executorService.submit(new ExclusiveOperation(txn4, recordID0));
        executorService.submit(new SharedOperation(txn1, recordID0));
        executorService.submit(new SharedOperation(txn2, recordID0));

        // give threads some time to release lock and acquire lock
        TimeUnit.SECONDS.sleep(3);
        executorService.shutdown();
        lockManager.close();
    }

    @Test
    public void largeSharedAndExclusiveLockSlowTransactionsTest() throws Exception {
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
                    TimeUnit.MILLISECONDS.sleep(100 + rand.nextInt(400));
                    lockManager.unlock(txn, recordID);
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
                    TimeUnit.MILLISECONDS.sleep(100 + rand.nextInt(400));
                    lockManager.unlock(txn, recordID);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        RecordID recordID0 = new RecordID(0, 0);

        for (int i = 0; i < 1000; i++) {
            Transaction txn = transactionManager.begin();
            if (Math.random() < 0.8) executorService.submit(new ExclusiveOperation(txn, recordID0));
            else executorService.submit(new SharedOperation(txn, recordID0));
        }

        // give threads some time to release lock and acquire lock
        TimeUnit.SECONDS.sleep(250);
        executorService.shutdown();
        lockManager.close();
    }

    @Test
    public void largeSharedAndExclusiveLockFastTransactionsTest() throws Exception {
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
                    TimeUnit.MILLISECONDS.sleep(10 + rand.nextInt(40));
                    lockManager.unlock(txn, recordID);
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
                    TimeUnit.MILLISECONDS.sleep(10 + rand.nextInt(40));
                    lockManager.unlock(txn, recordID);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        RecordID recordID0 = new RecordID(0, 0);

        for (int i = 0; i < 1000; i++) {
            Transaction txn = transactionManager.begin();
            if (Math.random() < 0.8) executorService.submit(new ExclusiveOperation(txn, recordID0));
            else executorService.submit(new SharedOperation(txn, recordID0));
        }

        // give threads some time to release lock and acquire lock
        TimeUnit.SECONDS.sleep(30);
        executorService.shutdown();
        lockManager.close();
    }

    @Test
    public void deadlockDetectionTest() throws Exception {

        class T0Operation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            private RecordID recordID1;
            public T0Operation(Transaction txn, RecordID recordID, RecordID recordID1) {
                this.txn = txn;
                this.recordID = recordID;
                this.recordID1 = recordID1;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    lockManager.acquireSharedLock(txn, recordID);
                    // simulate internal process for 100 milliseconds
                    TimeUnit.MILLISECONDS.sleep(100);
                    lockManager.acquireSharedLock(txn, recordID1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        class T1Operation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            private RecordID recordID1;
            public T1Operation(Transaction txn, RecordID recordID, RecordID recordID1) {
                this.txn = txn;
                this.recordID = recordID;
                this.recordID1 = recordID1;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    lockManager.acquireExclusiveLock(txn, recordID);
                    // simulate internal process for 200 milliseconds
                    TimeUnit.MILLISECONDS.sleep(200);
                    lockManager.acquireExclusiveLock(txn, recordID1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        class T2Operation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            private RecordID recordID1;
            public T2Operation(Transaction txn, RecordID recordID, RecordID recordID1) {
                this.txn = txn;
                this.recordID = recordID;
                this.recordID1 = recordID1;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    lockManager.acquireSharedLock(txn, recordID);
                    // simulate internal process for 300 milliseconds
                    TimeUnit.MILLISECONDS.sleep(300);
                    lockManager.acquireExclusiveLock(txn, recordID1);
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

        executorService.submit(new T0Operation(txn0, recordID0, recordID1));
        executorService.submit(new T1Operation(txn1, recordID1, recordID2));
        executorService.submit(new T2Operation(txn2, recordID2, recordID0));

        // give detector enough time to finish detecting
        TimeUnit.SECONDS.sleep(5);
        executorService.shutdown();
        lockManager.close();

        int[] expectArray = {2, 1, 0, 2};
        Stack<Integer> actualStack = lockManager.getCycles().get(0);
        for (int j = 0; j < actualStack.size(); j++) {
            assertEquals(actualStack.get(j), new Integer(expectArray[j]));
        }
    }

    @Test
    public void largeDeadlockDetectionTest() throws Exception {

        class T0Operation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            private RecordID recordID1;
            public T0Operation(Transaction txn, RecordID recordID, RecordID recordID1) {
                this.txn = txn;
                this.recordID = recordID;
                this.recordID1 = recordID1;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    lockManager.acquireSharedLock(txn, recordID);
                    // simulate internal process for 100 milliseconds
                    TimeUnit.MILLISECONDS.sleep(10);
                    lockManager.acquireSharedLock(txn, recordID1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        class T1Operation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            private RecordID recordID1;
            public T1Operation(Transaction txn, RecordID recordID, RecordID recordID1) {
                this.txn = txn;
                this.recordID = recordID;
                this.recordID1 = recordID1;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    lockManager.acquireExclusiveLock(txn, recordID);
                    // simulate internal process for 200 milliseconds
                    TimeUnit.MILLISECONDS.sleep(20);
                    lockManager.acquireExclusiveLock(txn, recordID1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        class T2Operation implements Callable<Boolean> {
            private Transaction txn;
            private RecordID recordID;
            private RecordID recordID1;
            public T2Operation(Transaction txn, RecordID recordID, RecordID recordID1) {
                this.txn = txn;
                this.recordID = recordID;
                this.recordID1 = recordID1;
            }

            @Override
            public Boolean call() {
                boolean res = false;
                try {
                    lockManager.acquireSharedLock(txn, recordID);
                    // simulate internal process for 300 milliseconds
                    TimeUnit.MILLISECONDS.sleep(30);
                    lockManager.acquireExclusiveLock(txn, recordID1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return res;
            }
        }

        RecordID recordID0 = new RecordID(0, 0);
        RecordID recordID1 = new RecordID(0, 1);
        RecordID recordID2 = new RecordID(0, 2);

        for (int i = 0; i < 1000; i++) {
            Transaction txn = transactionManager.begin();
            if (i % 3 == 0) executorService.submit(new T0Operation(txn, recordID0, recordID1));
            else if (i % 3 == 1) executorService.submit(new T1Operation(txn, recordID1, recordID2));
            else executorService.submit(new T2Operation(txn, recordID2, recordID0));
        }

        // give detector enough time to finish detecting
        TimeUnit.SECONDS.sleep(5);
        executorService.shutdown();
        lockManager.close();

        int[] expectArray = {2, 1, 0, 2};
        Stack<Integer> actualStack = lockManager.getCycles().get(0);
        for (int j = 0; j < actualStack.size(); j++) {
            assertEquals(actualStack.get(j), new Integer(expectArray[j]));
        }
    }
}
