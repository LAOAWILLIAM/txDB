package txDB.concurrency;

import txDB.storage.table.RecordID;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LockManager {
    // TODO: deadlock discovery with deadlock detection, and lock granularity support (now only tuple supported)
    public enum twoPhaseLockType {REGULAR, STRICT}
    public enum deadlockType {PREVENTION, DETECTION}
    public enum lockType {SHARED, EXCLUSIVE}
    private twoPhaseLockType tplt;
    private deadlockType dlt;
    private ExecutorService detectionExec;
    private AtomicBoolean whetherDetection;
    private DirectedGraph directedGraph;
    private WaitsForGraph waitsForGraph;
    private ArrayList<Stack<Integer>> cycles;
    private HashMap<RecordID, LockRequestQueue> lockTable;

    public LockManager(twoPhaseLockType tplt, deadlockType dlt) {
        this.tplt = tplt;
        this.dlt = dlt;
        this.lockTable = new HashMap<>();
        whetherDetection = new AtomicBoolean(false);

        if (dlt.equals(deadlockType.DETECTION)) {
            directedGraph = new DirectedGraph();
            cycles = new ArrayList<>();
            whetherDetection.set(true);
            detectionExec = Executors.newSingleThreadExecutor();
            detectionExec.execute(new cycleDetection());
        }
    }

    private class cycleDetection implements Runnable {
        @Override
        public void run() {
            System.out.println("detection is on");
            while (whetherDetection.get() && !Thread.interrupted()) {
                try {
                    synchronized (LockManager.this) {
                        waitsForGraph = new WaitsForGraph(directedGraph);
                        cycles = waitsForGraph.dfsFindCycle();
                    }
                    TimeUnit.MILLISECONDS.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public ArrayList<Stack<Integer>> getCycles() {
        return cycles;
    }

    private class LockRequestQueue {
        private Queue<LockRequest> requestQueue;
        private boolean isShared = true;
//        private Lock lock;
//        private Condition condition;

        public LockRequestQueue() {
            requestQueue = new LinkedList<>();
//            lock = new ReentrantLock();
//            condition = lock.newCondition();
        }

        public boolean isShared() {
            return isShared;
        }

        public void setShared(boolean b) {
            isShared = b;
        }

        public Queue<LockRequest> getRequestQueue() {
            return requestQueue;
        }

        public void pushRequestQueue(LockRequest lockRequest) {
            requestQueue.add(lockRequest);
        }

        public void popRequestQueue() {
            requestQueue.poll();
        }

        public void removeRequestQueue(LockRequest lockRequest) {
            requestQueue.remove(lockRequest);
        }

        public int size() {
            return requestQueue.size();
        }

        public void findAndRemoveRequestQueue(Transaction txn) {
            for (LockRequest lockRequest : requestQueue) {
                if (lockRequest.getTxnId() == txn.getTxnId()) {
                    if (lockRequest.getLockType() == lockType.SHARED) {
                        if (findGrantedTransaction(txn) != -1) {
                            setShared(true);
                        } else {
                            getOldestRequestLockType(txn);
                        }
                    } else {
                        getOldestRequestLockType(txn);
                    }
                    removeRequestQueue(lockRequest);
                    break;
                }
            }
        }

        private void getOldestRequestLockType(Transaction txn) {
            for (LockRequest lockRequest : requestQueue) {
                if (lockRequest.getTxnId() != txn.getTxnId()) {
                    if (!lockRequest.isGranted()) {
                        setShared(lockRequest.getLockType() == lockType.SHARED);
                        break;
                    }
                }
            }
        }

        public int findGrantedTransaction(Transaction txn) {
            for (LockRequest lockRequest : requestQueue) {
                if (lockRequest.getTxnId() != txn.getTxnId()) {
                    if (lockRequest.isGranted()) {
                        return lockRequest.getTxnId();
                    }
                }
            }
            return -1;
        }
    }

    private class LockRequest {
        private int txnId;
        private lockType lockType;
        private boolean granted;

        public LockRequest(int txnId, lockType lockType) {
            this.txnId = txnId;
            this.lockType = lockType;
            this.granted = false;
        }

        public boolean isGranted() {
            return granted;
        }

        public void setGranted(boolean granted) {
            this.granted = granted;
        }

        public int getTxnId() {
            return txnId;
        }

        public lockType getLockType() {
            return lockType;
        }
    }

    /**
     *
     * @param txn
     * @param recordID
     * @return
     * @throws InterruptedException
     */
    public boolean acquireSharedLock(Transaction txn, RecordID recordID) throws InterruptedException {
        // TODO
        synchronized (this) {
            LockRequest lockRequest = new LockRequest(txn.getTxnId(), lockType.SHARED);
            if (whetherDetection.get()) {
                addTxnNode(txn.getTxnId());
//                System.out.println("add node: " + txn.getTxnId());
            }

            if (!lockTable.containsKey(recordID)) {
                lockRequest.setGranted(true);
                LockRequestQueue lockRequestQueue = new LockRequestQueue();
                lockRequestQueue.pushRequestQueue(lockRequest);
                lockTable.put(recordID, lockRequestQueue);
                lockRequestQueue.setShared(true);
                notifyAll();
                System.out.println("txn " + txn.getTxnId() + " get shared lock on tuple " + recordID.getTupleIndex());
                return true;
            }

            LockRequestQueue lockRequestQueue = lockTable.get(recordID);
            lockRequestQueue.pushRequestQueue(lockRequest);

            int anyGrantedTransaction = lockRequestQueue.findGrantedTransaction(txn);
//            System.out.println("any granted txn when txn " + txn.getTxnId() + " is acquiring: " + anyGrantedTransaction + ", isShared: " + lockRequestQueue.isShared());

            // use different deadlock handling algorithm based on whetherDetection
            if (whetherDetection.get()) {
                while (!lockRequestQueue.isShared()) {
                    addTxnEdge(txn.getTxnId(), anyGrantedTransaction);
//                    System.out.println("txn " + txn.getTxnId() + " is waiting");
                    wait();
                }
            } else {
                // deadlock prevention using wait-die scheme
                if (!lockRequestQueue.isShared()) {
                    if (txn.getTxnId() > anyGrantedTransaction) {
                        System.out.println("txn " + txn.getTxnId() + " aborts");
                        txn.setTransactionState(Transaction.TransactionState.ABORTED);
                        return false;
                    } else if (txn.getTxnId() < anyGrantedTransaction) {
                        while (!lockRequestQueue.isShared()) {
//                            System.out.println("txn " + txn.getTxnId() + " is waiting");
                            wait();
                        }
                    }
                }
            }

            System.out.println("txn " + txn.getTxnId() + " get shared lock on tuple " + recordID.getTupleIndex());
            lockRequest.setGranted(true);
            notifyAll();

            return true;
        }
    }

    /**
     *
     * @param txn
     * @param recordID
     * @return
     * @throws InterruptedException
     */
    public boolean acquireExclusiveLock(Transaction txn, RecordID recordID) throws InterruptedException {
        // TODO
        synchronized (this) {
            LockRequest lockRequest = new LockRequest(txn.getTxnId(), lockType.EXCLUSIVE);
            if (whetherDetection.get()) {
                addTxnNode(txn.getTxnId());
//                System.out.println("add node: " + txn.getTxnId());
            }

            if (!lockTable.containsKey(recordID)) {
                lockRequest.setGranted(true);
                LockRequestQueue lockRequestQueue = new LockRequestQueue();
                lockRequestQueue.pushRequestQueue(lockRequest);
                lockTable.put(recordID, lockRequestQueue);
                lockRequestQueue.setShared(false);
                System.out.println("txn " + txn.getTxnId() + " get exclusive lock on tuple " + recordID.getTupleIndex());
                return true;
            }

            LockRequestQueue lockRequestQueue = lockTable.get(recordID);
            lockRequestQueue.pushRequestQueue(lockRequest);

            int anyGrantedTransaction = lockRequestQueue.findGrantedTransaction(txn);
//            System.out.println("any granted txn: " + txn.getTxnId() + " is acquiring: " + anyGrantedTransaction + ", isShared: " + lockRequestQueue.isShared());

            // use different deadlock handling algorithm based on whetherDetection
            if (whetherDetection.get()) {
                while (lockRequestQueue.isShared() || anyGrantedTransaction != -1) {
                    addTxnEdge(txn.getTxnId(), anyGrantedTransaction);
//                    System.out.println("txn " + txn.getTxnId() + " is waiting");
                    wait();
                    anyGrantedTransaction = lockRequestQueue.findGrantedTransaction(txn);
                }
            } else {
                // deadlock prevention using wait-die scheme
                if (lockRequestQueue.isShared() || anyGrantedTransaction != -1) {
                    if (txn.getTxnId() > anyGrantedTransaction) {
                        System.out.println("txn " + txn.getTxnId() + " aborts");
                        txn.setTransactionState(Transaction.TransactionState.ABORTED);
                        return false;
                    } else if (txn.getTxnId() < anyGrantedTransaction) {
                        while (lockRequestQueue.isShared() || anyGrantedTransaction != -1) {
//                            System.out.println("txn " + txn.getTxnId() + " is waiting");
                            wait();
                            anyGrantedTransaction = lockRequestQueue.findGrantedTransaction(txn);
                            if (txn.getTxnId() > anyGrantedTransaction) {
                                System.out.println("txn " + txn.getTxnId() + " aborts");
                                txn.setTransactionState(Transaction.TransactionState.ABORTED);
                                return false;
                            }
                        }
                    }
                }
            }

            System.out.println("txn " + txn.getTxnId() + " get exclusive lock on tuple " + recordID.getTupleIndex());
            lockRequest.setGranted(true);

            return true;
        }
    }

    /**
     *
     * @param txn
     * @param recordID
     * @return
     */
    public boolean unlock(Transaction txn, RecordID recordID) {
        // TODO
        synchronized (this) {
            System.out.println("txn " + txn.getTxnId() + " release lock");
            LockRequestQueue lockRequestQueue = lockTable.get(recordID);
            lockRequestQueue.findAndRemoveRequestQueue(txn);
            removeTxnNode(txn.getTxnId());
//            System.out.println("remove node: " + txn.getTxnId());
            notifyAll();

            return true;
        }
    }

    private void addTxnNode(int txn) {
        directedGraph.addNode(txn);
    }

    private void removeTxnNode(int txn) {
        directedGraph.removeNode(txn);
    }

    private void addTxnEdge(int txn1, int txn2) {
        directedGraph.addEdge(txn1, txn2);
    }

    private void removeTxnEdge(int txn1, int txn2) {
        directedGraph.removeEdge(txn1, txn2);
    }

    private ArrayList<Stack<Integer>> findCycle() {
        return cycles;
    }

    public void close() {
        if (whetherDetection.get()) detectionExec.shutdown();
    }
}
