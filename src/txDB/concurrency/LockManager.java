package txDB.concurrency;

enum twoPhaseLockType {REGULAR, STRICT}
enum deadlockType {PREVENTION, DETECTION}

public class LockManager {
    // TODO
    public enum lockType {SHARED, EXCLUSIVE}
    private twoPhaseLockType tplt;
    private deadlockType dlt;
    private Thread detectionThread;

    public LockManager(twoPhaseLockType tplt, deadlockType dlt) {
        this.tplt = tplt;
        this.dlt = dlt;

        if (dlt.equals(deadlockType.DETECTION)) {
            detectionThread = new Thread();
        }
    }


}
