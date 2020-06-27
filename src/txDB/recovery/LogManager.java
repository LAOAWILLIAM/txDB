package txDB.recovery;

import txDB.Config;
import txDB.storage.disk.DiskManager;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a runtime log manager, group commit is used here;
 * For thread coordination, please refer to relative knowledge accordingly,
 * which is very hard and meanwhile important.
 */
public class LogManager {
    // TODO
//    private ByteBuffer testFlushLogBuffer;
//    private ByteBuffer testAppendLogBuffer;
    private ByteBuffer flushLogBuffer;
    private ByteBuffer appendLogBuffer;
    private ByteBuffer tmpLogBuffer;
    private ByteBuffer curFlushLogBuffer;
    private DiskManager diskManager;
    private ExecutorService flushService;
    private AtomicInteger nextLsn;
    private AtomicBoolean whetherFlush;
    private AtomicInteger lastLsn;
    private AtomicInteger flushedLsn;

    /**
     *
     * @param diskManager
     */
    public LogManager(DiskManager diskManager) {
        flushLogBuffer = ByteBuffer.allocate(Config.LOG_SIZE);
        appendLogBuffer = ByteBuffer.allocate(Config.LOG_SIZE);
        curFlushLogBuffer = appendLogBuffer;
//        testAppendLogBuffer = appendLogBuffer;
//        testFlushLogBuffer = flushLogBuffer;
        this.diskManager = diskManager;
        // TODO: lsn should be persisted on disk
        nextLsn = new AtomicInteger(0);
        lastLsn = new AtomicInteger(Config.INVALID_LSN);
        flushedLsn = new AtomicInteger(Config.INVALID_LSN);

        whetherFlush = new AtomicBoolean(false);
        flushService = Executors.newSingleThreadExecutor();
//        this.periodicalFlushService.execute(new periodicalFlush());
    }

    /**
     *
     */
    private class flushThread implements Runnable {
        private final LogManager logManager;

        public flushThread(LogManager logManager) {
            this.logManager = logManager;
        }

        @Override
        public void run() {
            System.out.println("flush thread is on");
            while (Config.ENABLE_LOGGING && !Thread.interrupted()) {
                synchronized (this) {
                    while (!whetherFlush.get()) {
                        try {
                            this.wait(Config.LOGGING_TIMEOUT);
//                            System.out.println("flush after timeout");
                            // flush after timeout
                            _flushLogBuffer();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    if (whetherFlush.get()) {
//                        System.out.println("flush not after timeout");
                        // flush when awaken
                        _flushLogBuffer();
                        whetherFlush.set(false);
                        // must get lock on logManager itself here
                        synchronized (logManager) {
                            logManager.notify();
                        }
                    }
                }
            }
            System.out.println("flush thread is off");
        }
    }

    /**
     *
     */
    public void startFlushService() {
        if (Config.ENABLE_LOGGING) return;
        Config.ENABLE_LOGGING = true;
        this.flushService.execute(new flushThread(this));
    }

    /**
     *
     * @param logRecord
     * @param flushNow
     * @return
     */
    public int appendLogRecord(LogRecord logRecord, boolean flushNow) {
        // TODO
        synchronized (this) {
            if (logRecord.getLogSize() <= 0) return -1;
            int lsn = nextLsn.getAndIncrement();
            logRecord.setLsn(lsn);
//            System.out.println(logRecord.toString() + ", " + appendLogBuffer.position());
//            System.out.println(lsn + ": " + (appendLogBuffer == testAppendLogBuffer));
            if (logRecord.getLogSize() > appendLogBuffer.remaining()) {
//                System.out.println("append buffer is full");
//                flushLogBuffer();

//                if (appendLogBuffer == testAppendLogBuffer) {
//                    System.out.println("append is full");
//                } else {
//                    System.out.println("flush is full");
//                }

                whetherFlush.set(true);

                notify();

                while (Config.ENABLE_LOGGING && whetherFlush.get()) {
//                    System.out.println("waiting");
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

//                if (appendLogBuffer == testAppendLogBuffer) {
//                    System.out.println("use append when full");
//                } else {
//                    System.out.println("use full when full");
//                }
            }

//            System.out.println(lsn + ": " + (appendLogBuffer == testAppendLogBuffer));
            appendLogBuffer.put(Arrays.copyOfRange(
                    logRecord.getLogRecordBuffer().array(), 0, logRecord.getLogSize()));

            lastLsn.set(lsn);

            if (flushNow) {
                flushLogBuffer(false);
            }

            return lsn;
        }
    }

    /**
     *
     */
    public void flushLogBuffer(boolean whetherForce) {
        synchronized (this) {
            while (Config.ENABLE_LOGGING && whetherFlush.get()) {
//                System.out.println("waiting");
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (!whetherForce) {
                // not force, instead of using flush thread
                whetherFlush.set(true);
                notify();
            } else {
                // force to flush when writing dirty pages to disk
                _flushLogBuffer();
            }
        }
    }

    private void _flushLogBuffer() {
        synchronized (this) {
//            if (curFlushLogBuffer == testFlushLogBuffer) {
//                System.out.println("use flush");
//            } else {
//                System.out.println("use append");
//            }
            if (curFlushLogBuffer.remaining() < Config.LOG_SIZE) {
//                if (curFlushLogBuffer == testFlushLogBuffer) {
//                    System.out.println("use flush");
//                } else {
//                    System.out.println("use append");
//                }
//                diskManager.writeLog(curFlushLogBuffer.array());
                diskManager.writeLog(
                        Arrays.copyOfRange(curFlushLogBuffer.array(), 0, curFlushLogBuffer.position()));
//                System.out.println("flush to disk");
                flushedLsn.set(lastLsn.get());
                // TODO: optimization needed
                for (int i = 0; i < curFlushLogBuffer.limit(); i++) {
                    curFlushLogBuffer.put(i, (byte) 0);
                }
                curFlushLogBuffer.clear();
                curFlushLogBuffer = flushLogBuffer;
                swapLogBuffer();
            }
//            else {
//                System.out.println("nothing to flush");
//            }
        }
    }

    /**
     *
     */
    public void closeFlushService() {
        if (!Config.ENABLE_LOGGING) return;
        flushLogBuffer(false);
        Config.ENABLE_LOGGING = false;
        this.flushService.shutdown();
        try {
            while (!this.flushService.awaitTermination(Config.LOGGING_TIMEOUT, TimeUnit.MILLISECONDS)) {
                System.out.println("waiting for flush service to terminate");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("flush service is terminated");
    }

    public int getFlushedLsn() {
        return flushedLsn.get();
    }

    private void swapLogBuffer() {
        tmpLogBuffer = appendLogBuffer;
        appendLogBuffer = flushLogBuffer;
        flushLogBuffer = tmpLogBuffer;
    }
}
