package txDB.recovery;

import txDB.Config;
import txDB.storage.disk.DiskManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class LogManager {
    // TODO
    private ByteBuffer logbuffer1;
    private ByteBuffer logbuffer2;
    private DiskManager diskManager;
    private ExecutorService periodicalFlushService;
    private ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private ObjectOutput out;

    public LogManager(DiskManager diskManager) {
        this.logbuffer1 = ByteBuffer.allocate(Config.LOG_SIZE);
        this.logbuffer2 = ByteBuffer.allocate(Config.LOG_SIZE);
        this.diskManager = diskManager;
        try {
            this.out = new ObjectOutputStream(bos);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.periodicalFlushService = Executors.newSingleThreadExecutor();
        this.periodicalFlushService.execute(new periodicalFlush());
    }

    private class periodicalFlush implements Runnable {
        @Override
        public void run() {
            System.out.println("gap flush is on");
            while (!Thread.interrupted()) {
                try {
                    flushLogBuffer();
                    TimeUnit.MILLISECONDS.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public int appendLogRecord(LogRecord logRecord) {
        // TODO
        try {
            bos.reset();
            out.writeObject(logRecord);
            logbuffer1.put(bos.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return -1;
    }

    public void flushLogBuffer() {
        // TODO
        diskManager.writeLog(logbuffer1.array());
    }

    public void closePeriodicalFlush() {
        this.periodicalFlushService.shutdown();
    }
}
