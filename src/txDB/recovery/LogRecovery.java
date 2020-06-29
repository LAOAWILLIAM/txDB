package txDB.recovery;

import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;

/**
 * This is a recovery manager after system crash,
 * I use ARIES here.
 */
public class LogRecovery {
    // TODO
    private DiskManager diskManager;
    private BufferManager bufferManager;

    public LogRecovery(DiskManager diskManager, BufferManager bufferManager) {
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
    }
}
