package txDB.concurrency;


import java.util.ArrayList;

/**
 * This class is for deadlock detection by finding directed cycles
 */
public class WaitsForGraph<K> {
    // TODO: not finished and not thread-safe
    private ArrayList<ArrayList<K>> adj;
    private int edgeNum;
    private int nodeNum;

    public WaitsForGraph() {

    }

    /**
     * Use dfs to find directed cycles
     */
    public void dfs() {
        // TODO
    }

    public void addEdge() {
        // TODO
    }

    public void removeEdge() {
        // TODO
    }
}
