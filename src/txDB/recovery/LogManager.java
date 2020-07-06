package txDB.recovery;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.MetaDataPage;
import txDB.storage.page.Page;

import java.io.*;
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
public class LogManager implements Serializable {
    // TODO
//    private ByteBuffer testFlushLogBuffer;
//    private ByteBuffer testAppendLogBuffer;
    private transient ByteBuffer flushLogBuffer;
    private transient ByteBuffer appendLogBuffer;
    private transient ByteBuffer tmpLogBuffer;
    private transient ByteBuffer curFlushLogBuffer;
    private transient DiskManager diskManager;
    private transient ExecutorService flushService;
    private AtomicInteger nextLsn;
    private AtomicBoolean whetherFlush;
    private AtomicInteger lastLsn;
    private AtomicInteger flushedLsn;
    private AtomicBoolean whetherCheckpoint;
    private AtomicInteger logFileLength;

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
        whetherCheckpoint = new AtomicBoolean(false);
        logFileLength = new AtomicInteger(0);
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
    public int appendLogRecord(LogRecord logRecord, boolean flushNow, boolean whetherCheckpoint) {
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
                // flush commit or abort or checkpoint records to disk
                if (whetherCheckpoint) {
                    System.out.println("appending checkpoint record");
                    flushLogBuffer(true, true);
                    this.whetherCheckpoint.set(false);
                    return logFileLength.get();
                } else {
                    flushLogBuffer(false, false);

                    // make sure such records are permanently stored on disk
                    while (Config.ENABLE_LOGGING && whetherFlush.get()) {
//                    System.out.println("waiting");
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    // here we can tell the outside whether the txn is committed or aborted
                    System.out.println("commit or abort record has been on disk");
                }
            }

            return lsn;
        }
    }

    /**
     *
     */
    public void flushLogBuffer(boolean whetherForce, boolean whetherCheckpoint) {
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
//                this.whetherCheckpoint.set(whetherCheckpoint);
                notify();
            } else {
                // force to flush when writing dirty pages to disk
                this.whetherCheckpoint.set(whetherCheckpoint);
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
                int logFileLength = diskManager.writeLog(
                        Arrays.copyOfRange(curFlushLogBuffer.array(), 0, curFlushLogBuffer.position()), whetherCheckpoint.get());
                if (whetherCheckpoint.get() && logFileLength != -1) {
//                    System.out.println("record checkpoint");
                    this.logFileLength.set(logFileLength);
                }
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
        flushLogBuffer(false, false);
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

    public int getNextLsn() {
        return nextLsn.get();
    }

    private void swapLogBuffer() {
        tmpLogBuffer = appendLogBuffer;
        appendLogBuffer = flushLogBuffer;
        flushLogBuffer = tmpLogBuffer;
    }

    // belows are test helper functions

    /**
     * Simulate a system crash that log buffers are cleaned
     */
    public void logCrash() {
        flushService.shutdown();
        for (int i = 0; i < appendLogBuffer.limit(); i++) {
            appendLogBuffer.put(i, (byte) 0);
        }
        appendLogBuffer.clear();
        for (int i = 0; i < flushLogBuffer.limit(); i++) {
            flushLogBuffer.put(i, (byte) 0);
        }
        flushLogBuffer.clear();
    }
}
