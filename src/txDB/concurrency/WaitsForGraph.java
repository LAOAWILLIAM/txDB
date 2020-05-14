package txDB.concurrency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Stack;

/**
 * This class is for deadlock detection by finding directed cycles
 */
public class WaitsForGraph {

//    private boolean[] marked;
    private HashMap<Integer, Boolean> markedMap;
//    private int[] edgeTo;
    private HashMap<Integer, Integer> edgeToMap;
    private Stack<Integer> cycle;
//    private boolean[] onStack;
    private HashMap<Integer, Boolean> onStackMap;
//    private int nodeNum;
    private DirectedGraph dg;

    public WaitsForGraph(DirectedGraph dg) {
        this.dg = dg;
//        this.nodeNum = dg.getNodeNum();
//        this.onStack = new boolean[nodeNum];
//        this.edgeTo = new int[nodeNum];
//        this.marked = new boolean[nodeNum];

        this.markedMap = new HashMap<>();
        this.edgeToMap = new HashMap<>();
        this.onStackMap = new HashMap<>();

        for (Integer v : dg.getAdj().keySet()) {
            markedMap.put(v, false);
            edgeToMap.put(v, 0);
            onStackMap.put(v, false);
        }
    }

    /**
     * Use dfs to find directed cycles
     */
    public ArrayList<Stack<Integer>> dfsFindCycle() {
        ArrayList<Stack<Integer>> cycles = new ArrayList<>();
//        for (int v = 0; v < nodeNum; v++) {
//            if (!marked[v]) dfs(v, cycles);
//        }

        for (Integer v : dg.getAdj().keySet()) {
            if (!markedMap.get(v)) dfs(v, cycles);
        }
        System.out.println(cycles);
        return cycles;
    }

    private void dfs(int v, ArrayList<Stack<Integer>> cycles) {
//        onStack[v] = true;
//        marked[v] = true;
        onStackMap.replace(v, true);
        markedMap.replace(v, true);
        for (int w : dg.getAdj().get(v)) {
//            if (!marked[w]) {
//                edgeTo[w] = v;
//                this.dfs(w, cycles);
//            } else if (onStack[w]) {
//                cycle = new Stack<>();
//                for (int x = v; x != w; x = edgeTo[x])
//                    cycle.push(x);
//                cycle.push(w);
//                cycle.push(v);
//                cycles.add(cycle);
//            }
            // in case node w has been removed
            if (markedMap.get(w) == null) return;
            if (!markedMap.get(w)) {
                edgeToMap.replace(w, v);
                this.dfs(w, cycles);
            } else if (onStackMap.get(w)) {
                cycle = new Stack<>();
                for (int x = v; x != w; x = edgeToMap.get(x))
                    cycle.push(x);
                cycle.push(w);
                cycle.push(v);
                cycles.add(cycle);
            }
        }
//        onStack[v] = false;
        onStackMap.replace(v, false);
    }
}
