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
 * This is a runtime log manager, group commit is used here
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

        whetherFlush = new AtomicBoolean(false);
        periodicalFlushService = Executors.newSingleThreadExecutor();
//        this.periodicalFlushService.execute(new periodicalFlush());
    }

    /**
     *
     */
    private class periodicalFlush implements Runnable {
        @Override
        public void run() {
            System.out.println("periodical flush is on");
            while (Config.ENABLE_LOGGING && !Thread.interrupted()) {
                // buffer full trigger
                if (whetherFlush.get()) {
//                    System.out.println("flush not after timeout");
                    flushLogBuffer();
                } else {
                    // TODO: timeout trigger needs redesign
                    try {
                        TimeUnit.MICROSECONDS.sleep(500);
//                        System.out.println("timeout");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     *
     */
    public void startPeriodicalFlush() {
        Config.ENABLE_LOGGING = true;
        this.periodicalFlushService.execute(new periodicalFlush());
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
        synchronized (curLogBuffer) {
            if (logRecord.getLogSize() <= 0) return -1;
            int lsn = nextLsn.getAndIncrement();
            logRecord.setLsn(lsn);
//            System.out.println(logRecord.toString() + ", " + curLogBuffer.position());
            if (logRecord.getLogSize() > curLogBuffer.remaining()) {
                whetherFlush.set(true);
                curLogBuffer.notify();

                while (whetherFlush.get()) {
//                    System.out.println("waiting");
                    curLogBuffer.wait();
                }

                if (curLogBuffer.equals(logbuffer1)) {
                    curLogBuffer = logbuffer2;
                } else {
                    curLogBuffer = logbuffer1;
                }

            }

//            if (logRecord.getLogRecordType() == LogRecord.LogRecordType.UPDATE) {
//                runtimeUndoMap.put(logRecord.getLsn(), logRecord);
//            }

            curLogBuffer.put(Arrays.copyOfRange(
                    logRecord.getLogRecordBuffer().array(), 0, logRecord.getLogSize()));

            if (flushNow) {
                flushLogBuffer();
            }

            return lsn;
        }
    }

    /**
     *
     */
    public void flushLogBuffer() {
        synchronized (curLogBuffer) {
            if (curLogBuffer.remaining() < Config.LOG_SIZE) {
//                System.out.println("cur pos: " + curLogBuffer.position());
//                diskManager.writeLog(
//                        Arrays.copyOfRange(curLogBuffer.array(), 0, curLogBuffer.position()));
                diskManager.writeLog(curLogBuffer.array());
                // TODO: optimization needed
                for (int i = 0; i < curLogBuffer.limit(); i++) {
                    curLogBuffer.put(i, (byte) 0);
                }
                curLogBuffer.clear();
                whetherFlush.set(false);
                curLogBuffer.notify();
            }
        }
    }

    /**
     *
     */
    public void closePeriodicalFlush() {
        Config.ENABLE_LOGGING = false;
        flushLogBuffer();
        this.periodicalFlushService.shutdown();
    }
}
