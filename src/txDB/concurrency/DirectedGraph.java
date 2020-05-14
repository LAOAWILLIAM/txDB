package txDB.concurrency;

import java.util.ArrayList;
import java.util.HashMap;

public class DirectedGraph {
    private HashMap<Integer, ArrayList<Integer>> adj;
    private int edgeNum;
    private int nodeNum;
    private int totalNodeNum;

    public DirectedGraph() {
        this.adj = new HashMap<>();
        this.edgeNum = 0;
        this.nodeNum = 0;
        this.totalNodeNum = 0;
    }

    public int getEdgeNum() {
        return edgeNum;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public int getTotalNodeNum() {
        return totalNodeNum;
    }

    public HashMap<Integer, ArrayList<Integer>> getAdj() {
        return adj;
    }

    public void addNode(int v) {
        if (!adj.containsKey(v)) {
            adj.put(v, new ArrayList<>());
            totalNodeNum = ++nodeNum;
        }
    }

    public void removeNode(int v) {
        if (adj.containsKey(v)) {
            adj.remove(v);
            nodeNum--;
        }
    }

    public void addEdge(int v, int w) {
        if (!adj.get(v).contains(w)) {
            adj.get(v).add(w);
            edgeNum++;
        }
    }

    public void removeEdge(int v, int w) {
        adj.get(v).remove((Integer) w);
        edgeNum--;
    }

    private Iterable<Integer> getAdj(int v) {
        return adj.get(v);
    }

    public void getAllAdj() {
        for (int i = 0; i < nodeNum; i++) {
            System.out.println(getAdj(i));
        }
    }
}
