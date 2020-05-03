package test.concurrency;

import org.junit.Test;
import static org.junit.Assert.*;

import txDB.concurrency.DirectedGraph;
import txDB.concurrency.WaitsForGraph;

public class WaitsForGraphTest {
    @Test
    public void directedGraphTest() {
        DirectedGraph directedGraph = new DirectedGraph();
        directedGraph.addNode(0);
        directedGraph.addNode(0);
        directedGraph.addNode(1);
        directedGraph.addNode(2);
        directedGraph.addNode(3);
        assertEquals(4, directedGraph.getNodeNum());

        directedGraph.addEdge(0, 1);
        directedGraph.addEdge(0, 2);
        directedGraph.addEdge(1, 3);
        directedGraph.addEdge(3, 2);
        assertEquals(4, directedGraph.getEdgeNum());

        directedGraph.removeEdge(0, 2);
        assertEquals(3, directedGraph.getEdgeNum());

        directedGraph.getAllAdj();
    }

    @Test
    public void cycleFindTest() {
        DirectedGraph directedGraph = new DirectedGraph();
        directedGraph.addNode(0);
        directedGraph.addNode(1);
        directedGraph.addNode(2);
        directedGraph.addNode(3);
        directedGraph.addNode(4);
        directedGraph.addNode(5);
        directedGraph.addNode(6);
        directedGraph.addNode(7);

        directedGraph.addEdge(0, 1);
        directedGraph.addEdge(0, 4);
        directedGraph.addEdge(1, 2);
        directedGraph.addEdge(2, 3);
        directedGraph.addEdge(3, 1);
        directedGraph.addEdge(5, 0);
        directedGraph.addEdge(4, 7);
        directedGraph.addEdge(7, 6);
        directedGraph.addEdge(6, 5);

        directedGraph.getAllAdj();

        WaitsForGraph waitsForGraph = new WaitsForGraph(directedGraph);
        waitsForGraph.dfsFindCycle();
    }
}
