package txDB;

public class Config {
    /**
     * Note:
     * In macOS 10.12, 1kb = 1000byte, however, conventionally, 1kb = 1024byte;
     * Here, I just use 1kb = 1024byte.
     */
    public static final int PAGE_SIZE = 4096; // page size in byte
    public static final int INVALID_PAGE_ID = -1;
    public static final boolean ENABLE_LOGGING = true;

    public Config() {}
}
