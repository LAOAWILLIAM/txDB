package txDB.concurrency;

import txDB.storage.table.RecordID;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

        public boolean getIsShared() {
            return isShared;
        }

        public void setIsShared(boolean b) {
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

        public int size() {
            return requestQueue.size();
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
    }

    public boolean acquireSharedLock(Transaction txn, RecordID recordID) throws InterruptedException {
        synchronized (this) {
            LockRequest lockRequest = new LockRequest(txn.getTxnId(), lockType.SHARED);

            if (!lockTable.containsKey(recordID)) {
                LockRequestQueue lockRequestQueue = new LockRequestQueue();
                lockRequestQueue.pushRequestQueue(lockRequest);
                lockTable.put(recordID, lockRequestQueue);
                lockRequestQueue.setIsShared(true);
                notifyAll();
                System.out.println("txn " + txn.getTxnId() + " get shared lock");
                return true;
            }

            LockRequestQueue lockRequestQueue = lockTable.get(recordID);
            lockRequestQueue.pushRequestQueue(lockRequest);

            while (!lockRequestQueue.getIsShared()) {
                System.out.println("txn " + txn.getTxnId() + " is waiting");
                wait();
            }

            System.out.println("txn " + txn.getTxnId() + " get shared lock");

            return true;
        }
    }

    public boolean acquireExclusiveLock(Transaction txn, RecordID recordID) throws InterruptedException {
        // TODO
        synchronized (this) {
            LockRequest lockRequest = new LockRequest(txn.getTxnId(), lockType.EXCLUSIVE);

            if (!lockTable.containsKey(recordID)) {
                LockRequestQueue lockRequestQueue = new LockRequestQueue();
                lockRequestQueue.pushRequestQueue(lockRequest);
                lockTable.put(recordID, lockRequestQueue);
                lockRequestQueue.setIsShared(false);
                System.out.println("txn " + txn.getTxnId() + " get exclusive lock");
                return true;
            }

            LockRequestQueue lockRequestQueue = lockTable.get(recordID);
            lockRequestQueue.pushRequestQueue(lockRequest);

            while (lockRequestQueue.getIsShared()) {
                System.out.println("txn " + txn.getTxnId() + " is waiting");
                wait();
            }

            System.out.println("txn " + txn.getTxnId() + " get exclusive lock");

            return true;
        }
    }

    public boolean unlock(Transaction txn, RecordID recordID) throws InterruptedException {
        // TODO
        return true;
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
