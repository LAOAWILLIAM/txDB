package txDB;

public class Config {
    /**
     * Note:
     * In macOS 10.12, 1kb = 1000byte, however, conventionally, 1kb = 1024byte;
     * Here, I just use 1kb = 1024byte.
     */
    public static final int PAGE_SIZE = 4096;       // page size in byte, 4 KB
    public static final int LOG_SIZE = 1024; // 209715200;   // log size in byte, 200 MB
    public static final int INVALID_PAGE_ID = -1;
    public static final int INVALID_LSN = -1;
    public static final int INVALID_TXN_ID = -1;
    public static final boolean ENABLE_LOGGING = true;

    public Config() {}
}
