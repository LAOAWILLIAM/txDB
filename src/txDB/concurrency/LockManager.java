package txDB.concurrency;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

enum twoPhaseLockType {REGULAR, STRICT}
enum deadlockType {PREVENTION, DETECTION}

public class LockManager {
    // TODO
    public enum lockType {SHARED, EXCLUSIVE}
    private twoPhaseLockType tplt;
    private deadlockType dlt;
    private ExecutorService detectionExec;
    private AtomicBoolean whetherDetection;

    public LockManager(twoPhaseLockType tplt, deadlockType dlt) {
        this.tplt = tplt;
        this.dlt = dlt;

        if (dlt.equals(deadlockType.DETECTION)) {
            whetherDetection = new AtomicBoolean(true);
            detectionExec = Executors.newSingleThreadExecutor();
            detectionExec.execute(new cycleDetection());
        }
    }

    private class cycleDetection implements Runnable {
        @Override
        public void run() {
            while (whetherDetection.get()) {

            }
        }
    }

    public void close() {
        detectionExec.shutdown();
    }
}
