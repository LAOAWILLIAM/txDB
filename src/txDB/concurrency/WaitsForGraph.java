package txDB.concurrency;

import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Stack;

/**
 * This class is for deadlock detection by finding directed cycles
 */
public class WaitsForGraph {

    private boolean[] marked;
    private int[] edgeTo;
    private Stack<Integer> cycle;
    private boolean[] onStack;
    private int nodeNum;
    private DirectedGraph dg;

    public WaitsForGraph(DirectedGraph dg) {
        this.dg = dg;
        this.nodeNum = dg.getNodeNum();
        this.onStack = new boolean[nodeNum];
        this.edgeTo = new int[nodeNum];
        this.marked = new boolean[nodeNum];
    }

    /**
     * Use dfs to find directed cycles
     */
    public ArrayList<Stack<Integer>> dfsFindCycle() {
        ArrayList<Stack<Integer>> cycles = new ArrayList<>();
        for (int v = 0; v < nodeNum; v++) {
            if (!marked[v]) dfs(v, cycles);
        }
        System.out.println(cycles);
        return cycles;
    }

    private void dfs(int v, ArrayList<Stack<Integer>> cycles) {
        onStack[v] = true;
        marked[v] = true;
        ListIterator<Integer> iterator = dg.getAdj().get(v).listIterator();
        while (iterator.hasNext()) {
            int w = iterator.next();
            if (!marked[w]) {
                edgeTo[w] = v;
                this.dfs(w, cycles);
            } else if (onStack[w]) {
                cycle = new Stack<>();
                for (int x = v; x != w; x = edgeTo[x])
                    cycle.push(x);
                cycle.push(w);
                cycle.push(v);
                cycles.add(cycle);
            }
        }
        onStack[v] = false;
    }
}
