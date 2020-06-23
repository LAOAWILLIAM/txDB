package txDB.recovery;

import txDB.Config;
import txDB.storage.disk.DiskManager;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
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
    private ByteBuffer logbuffer1;
    private ByteBuffer logbuffer2;
    private ByteBuffer curLogBuffer;
    private DiskManager diskManager;
    private ExecutorService periodicalFlushService;
    private AtomicInteger nextLsn;
    private AtomicBoolean whetherFlush;
    private AtomicInteger lastLsn;
    private AtomicInteger flushedLsn;

    /**
     *
     * @param diskManager
     */
    public LogManager(DiskManager diskManager) {
        logbuffer1 = ByteBuffer.allocate(Config.LOG_SIZE);
        logbuffer2 = ByteBuffer.allocate(Config.LOG_SIZE);
        curLogBuffer = logbuffer1;
        this.diskManager = diskManager;
        // TODO: lsn should be persisted on disk
        nextLsn = new AtomicInteger(0);
        lastLsn = new AtomicInteger(Config.INVALID_LSN);
        flushedLsn = new AtomicInteger(Config.INVALID_LSN);

        whetherFlush = new AtomicBoolean(false);
        periodicalFlushService = Executors.newSingleThreadExecutor();
//        this.periodicalFlushService.execute(new periodicalFlush());
    }

    /**
     *
     */
    private class periodicalFlush implements Runnable {
        private final LogManager logManager;

        public periodicalFlush(LogManager logManager) {
            this.logManager = logManager;
        }

        @Override
        public void run() {
            System.out.println("periodical flush is on");
            while (Config.ENABLE_LOGGING && !Thread.interrupted()) {
                synchronized (this) {
                    // buffer full trigger
                    if (whetherFlush.get()) {
//                        System.out.println("flush not after timeout");
                        _flushLogBuffer();
                        whetherFlush.set(false);
                        // must get lock on logManager itself here
                        synchronized (logManager) {
                            logManager.notify();
                        }
                    } else {
                        try {
                            TimeUnit.MICROSECONDS.sleep(500);
//                            System.out.println("timeout");
                            _flushLogBuffer();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            System.out.println("periodical flush is off");
        }
    }

    /**
     *
     */
    public void startPeriodicalFlush() {
        if (Config.ENABLE_LOGGING) return;
        Config.ENABLE_LOGGING = true;
        this.periodicalFlushService.execute(new periodicalFlush(this));
    }

    /**
     *
     * @param logRecord
     * @param flushNow
     * @return
     * @throws InterruptedException
     */
    public int appendLogRecord(LogRecord logRecord, boolean flushNow) throws InterruptedException {
        // TODO
        synchronized (this) {
            if (logRecord.getLogSize() <= 0) return -1;
            int lsn = nextLsn.getAndIncrement();
            logRecord.setLsn(lsn);
//            System.out.println(logRecord.toString() + ", " + curLogBuffer.position());
            if (logRecord.getLogSize() > curLogBuffer.remaining()) {
                flushLogBuffer();

                if (curLogBuffer.equals(logbuffer1)) {
                    curLogBuffer = logbuffer2;
                } else {
                    curLogBuffer = logbuffer1;
                }

            }

            curLogBuffer.put(Arrays.copyOfRange(
                    logRecord.getLogRecordBuffer().array(), 0, logRecord.getLogSize()));

            lastLsn.set(lsn);

            if (flushNow) {
                flushLogBuffer();
            }

            return lsn;
        }
    }

    /**
     *
     */
    public void flushLogBuffer() throws InterruptedException {
        synchronized (this) {
            whetherFlush.set(true);
            notify();

            while (Config.ENABLE_LOGGING && whetherFlush.get()) {
//                System.out.println("waiting");
                wait();
            }
        }
    }

    private void _flushLogBuffer() {
        synchronized (this) {
            if (curLogBuffer.remaining() < Config.LOG_SIZE) {
                diskManager.writeLog(curLogBuffer.array());
                flushedLsn.set(lastLsn.get());
                // TODO: optimization needed
                for (int i = 0; i < curLogBuffer.limit(); i++) {
                    curLogBuffer.put(i, (byte) 0);
                }
                curLogBuffer.clear();
            }
        }
    }

    /**
     *
     */
    public void closePeriodicalFlush() throws InterruptedException {
        if (!Config.ENABLE_LOGGING) return;
        flushLogBuffer();
        Config.ENABLE_LOGGING = false;
        this.periodicalFlushService.shutdown();
    }

    public int getFlushedLsn() {
        return flushedLsn.get();
    }
}
