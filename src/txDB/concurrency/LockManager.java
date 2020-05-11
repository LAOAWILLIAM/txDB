package txDB.concurrency;

import txDB.storage.table.RecordID;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LockManager {
    // TODO
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
        this.directedGraph = new DirectedGraph();
        this.cycles = new ArrayList<>();
        this.lockTable = new HashMap<>();

        if (dlt.equals(deadlockType.DETECTION)) {
            whetherDetection = new AtomicBoolean(true);
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
                    waitsForGraph = new WaitsForGraph(directedGraph);
                    cycles = waitsForGraph.dfsFindCycle();
                    TimeUnit.MILLISECONDS.sleep(1000);
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
//                    if (lockRequest.getLockType() == lockType.SHARED) {
//                        setShared(false);
//                    } else {
//                        setShared(true);
//                    }
                    setShared(true);
                    removeRequestQueue(lockRequest);
                    break;
                }
            }
        }

        public boolean findGrantedTransaction(Transaction txn) {
            for (LockRequest lockRequest : requestQueue) {
                if (lockRequest.getTxnId() != txn.getTxnId()) {
                    if (lockRequest.isGranted()) {
                        addTxnEdge(txn.getTxnId(), lockRequest.getTxnId());
                        return true;
                    }
                }
            }
            return false;
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

    public boolean acquireSharedLock(Transaction txn, RecordID recordID) throws InterruptedException {
        // TODO
        synchronized (this) {
            LockRequest lockRequest = new LockRequest(txn.getTxnId(), lockType.SHARED);
            addTxnNode(txn.getTxnId());

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

            boolean anyGrantedTransaction = lockRequestQueue.findGrantedTransaction(txn);
            System.out.println("any granted txn: " + anyGrantedTransaction);

            while (!lockRequestQueue.isShared()) {
                System.out.println("txn " + txn.getTxnId() + " is waiting");
                wait();
            }

            System.out.println("txn " + txn.getTxnId() + " get shared lock on tuple " + recordID.getTupleIndex());
            lockRequest.setGranted(true);
            notifyAll();

            return true;
        }
    }

    public boolean acquireExclusiveLock(Transaction txn, RecordID recordID) throws InterruptedException {
        // TODO
        synchronized (this) {
            LockRequest lockRequest = new LockRequest(txn.getTxnId(), lockType.EXCLUSIVE);
            addTxnNode(txn.getTxnId());

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

            boolean anyGrantedTransaction = lockRequestQueue.findGrantedTransaction(txn);
            System.out.println("any granted txn: " + anyGrantedTransaction);

            while (lockRequestQueue.isShared() && anyGrantedTransaction) {
                System.out.println("txn " + txn.getTxnId() + " is waiting");
                wait();
            }

            System.out.println("txn " + txn.getTxnId() + " get exclusive lock on tuple " + recordID.getTupleIndex());
            lockRequest.setGranted(true);

            return true;
        }
    }

    public boolean unlock(Transaction txn, RecordID recordID) throws InterruptedException {
        // TODO
        synchronized (this) {
            System.out.println("txn " + txn.getTxnId() + " release lock");
            LockRequestQueue lockRequestQueue = lockTable.get(recordID);
            lockRequestQueue.findAndRemoveRequestQueue(txn);
            notifyAll();

            return true;
        }
    }

    public void addTxnNode(int txn) {
        directedGraph.addNode(txn);
    }

    public void removeTxnNode(int txn) {
        directedGraph.removeNode(txn);
    }

    public void addTxnEdge(int txn1, int txn2) {
        directedGraph.addEdge(txn1, txn2);
    }

    public void removeTxnEdge(int txn1, int txn2) {
        directedGraph.removeEdge(txn1, txn2);
    }

    public ArrayList<Stack<Integer>> findCycle() {
        return cycles;
    }

    public void close() {
        detectionExec.shutdown();
    }
}
