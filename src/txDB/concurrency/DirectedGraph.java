package txDB.concurrency;

import java.util.ArrayList;

public class DirectedGraph {
    private ArrayList<ArrayList<Integer>> adj;
    private int edgeNum;
    private int nodeNum;

    public DirectedGraph() {
        this.adj = new ArrayList<>();
        this.edgeNum = 0;
        this.nodeNum = 0;
    }

    public int getEdgeNum() {
        return edgeNum;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public ArrayList<ArrayList<Integer>> getAdj() {
        return adj;
    }

    public void addNode(int v) {
        if (adj.size() <= v) {
            adj.add(v, new ArrayList<>());
            nodeNum++;
        }
    }

    public void addEdge(int v, Integer w) {
        adj.get(v).add(w);
        edgeNum++;
    }

    public void removeEdge(int v, Integer w) {
        adj.get(v).remove(w);
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
