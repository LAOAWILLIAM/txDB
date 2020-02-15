package txDB.concurrency;

public class TransactionManager implements Runnable {
    // TODO

    private Thread thread;

    @Override
    public void run() {

    }

    public void start() {
        thread = new Thread(this);
        thread.start();
    }

}
