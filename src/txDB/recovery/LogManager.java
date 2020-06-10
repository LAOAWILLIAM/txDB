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
 * Group committing is used here
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

    private class periodicalFlush implements Runnable {
        @Override
        public void run() {
            System.out.println("periodical flush is on");
            while (!Thread.interrupted()) {
                if (whetherFlush.get()) {
//                    System.out.println("flush");
                    flushLogBuffer();
                } else {
//                    try {
//                        TimeUnit.MICROSECONDS.sleep(2000);
//                        System.out.println("timeout");
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }
            }
        }
    }

    public void startPeriodicalFlush() {
        this.periodicalFlushService.execute(new periodicalFlush());
    }

    public int appendLogRecord(LogRecord logRecord) throws InterruptedException {
        // TODO
        synchronized (curLogBuffer) {
            logRecord.setLsn(nextLsn.getAndIncrement());
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

            curLogBuffer.put(Arrays.copyOfRange(
                    logRecord.getLogRecordBuffer().array(), 0, logRecord.getLogSize()));

            return nextLsn.get();
        }
    }

    public void flushLogBuffer() {
        synchronized (curLogBuffer) {
            if (curLogBuffer.remaining() < Config.LOG_SIZE) {
//                System.out.println("cur pos: " + curLogBuffer.position());
                diskManager.writeLog(
                        Arrays.copyOfRange(curLogBuffer.array(), 0, curLogBuffer.position()));
                curLogBuffer.clear();
                whetherFlush.set(false);
                curLogBuffer.notify();
            }
        }
    }

    public void closePeriodicalFlush() {
        flushLogBuffer();
        this.periodicalFlushService.shutdown();
    }
}
