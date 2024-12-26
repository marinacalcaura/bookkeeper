package org.apache.bookkeeper.net;

import org.junit.*;
import static org.junit.Assert.*;

import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

// Class for the test
@RunWith(value = Enclosed.class)
public class NetworkTopologyImplTest {

    @RunWith(value = Parameterized.class)
    public static class AddNodeTest{

        private Node node;
        private boolean expectException;
        private Class<? extends Exception> expectedExceptionType;

        private NetworkTopologyImpl sut;

        public AddNodeTest(Node node, boolean expectException, Class<? extends Exception> expectedExceptionType) {
            this.node = node;
            this.expectException = expectException;
            this.expectedExceptionType = expectedExceptionType;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {

                    {new NodeBase("127.0.0.1:5555", "/root"), false, null},
                    {new NetworkTopologyImpl.InnerNode("127.0.0.1:4001", "/root/inner"), true, IllegalArgumentException.class},
                    {null, false, null},
                    //{new NodeBase("127.0.0.1:4002", "/root/127.0.0.1:3999"), true, IllegalArgumentException.class},
                    {new NodeBase("127.0.0.1:4002", "/root/first"), true, NetworkTopologyImpl.InvalidTopologyException.class},

            });
        }

        // Setup prima di ogni test
        @Before
        public void setUp() {
            sut = new NetworkTopologyImpl();

            //lo aggiungo cosi cambio la profondità
            if (expectException && expectedExceptionType == NetworkTopologyImpl.InvalidTopologyException.class) {
                String rack = node.getNetworkLocation() + "/rack/127.0.0.1:4003";
                sut.add(new NodeBase(rack));
            }

            /*if (expectException && expectedExceptionType == IllegalArgumentException.class) {
                sut.add(new NodeBase("127.0.0.1:3999", "/root"));
            }*/
        }

        @Test
        public void testAdd() {
            if (expectException) {
                //verifico che l'eccezione attesa venga lanciata
                assertThrows(expectedExceptionType, () -> {
                    sut.add(node);
                });
            } else {

                if (node == null) {
                    sut.add(node);
                    assertFalse(sut.contains(node));
                } else {
                    sut.add(node);
                    assertTrue(sut.contains(node));
                }
            }
        }
    }

    @RunWith(value = Parameterized.class)
    public static class RemoveNodeTest {

        private Node node;
        private boolean expectException;
        private Class<? extends Exception> expectedExceptionType;

        private NetworkTopologyImpl sut;


        public RemoveNodeTest(Node node, boolean expectException, Class<? extends Exception> expectedExceptionType) {
            this.node = node;
            this.expectException = expectException;
            this.expectedExceptionType = expectedExceptionType;
        }


        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {

                    {new NodeBase("127.0.0.1:5555", "/root"), false, null},
                    {new NetworkTopologyImpl.InnerNode("127.0.0.1:5556", "/root/inner"), true, IllegalArgumentException.class },
                    {null, false, null},

            });
        }

        // Setup prima di ogni test
        @Before
        public void setUp() {
            sut = new NetworkTopologyImpl();
        }
        @Test
        public void testRemove() {

            if (expectException) {
                assertThrows(expectedExceptionType, () -> {
                    sut.remove(node);
                });
            } else {

                if (!sut.contains(node)) {
                    sut.add(node); //agg il nodo specifico da rimuovere
                }
                sut.remove(node);
                assertFalse(sut.contains(node));

            }
        }
    }

    @RunWith(value = Parameterized.class)
    public static class TopologyTest {

        private Node node1;
        private Node node2;
        private boolean expectException;
        private Class<? extends Exception> expectedExceptionType;

        private NetworkTopologyImpl sut;


        public TopologyTest(Node node1, Node node2, boolean expectException, Class<? extends Exception> expectedExceptionType) {
            this.node1 = node1;
            this.node2 = node2;
            this.expectException = expectException;
            this.expectedExceptionType = expectedExceptionType;
        }


        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {

                    {new NodeBase("127.0.0.1:4001", "/root/rack1"),new NodeBase("127.0.0.1:4002", "/root/rack1"), false, null},
                    {new NodeBase("127.0.0.1:4001", "/root/rack1"),new NodeBase("127.0.0.1:4002", "/root/rack2"), false, null},
                    //{null, null, true, IllegalArgumentException.class},
                    //{null,new NodeBase("127.0.0.1:4002", "/root/rack2"),true,IllegalArgumentException.class}

            });
        }

        // Setup prima di ogni test
        @Before
        public void setUp() {
            sut = new NetworkTopologyImpl();
        }
        @Test
        public void testIsOnSameRack() {

            sut.add(node1);
            sut.add(node2);

            if (expectException) {
                assertThrows(expectedExceptionType, () -> {
                    sut.isOnSameRack(node1,node2);
                });
            } else {

                boolean result = sut.isOnSameRack(node1,node2);
                // Aggiungi la stampa per visualizzare le locazioni di rete
                System.out.println("Node 1 Location: " + node1.getNetworkLocation());
                System.out.println("Node 2 Location: " + node2.getNetworkLocation());
                boolean expected = node1.getNetworkLocation().equals(node2.getNetworkLocation());
                // Stampare anche il risultato previsto e ottenuto
                System.out.println("Expected result: " + expected);
                System.out.println("Actual result: " + result);
                Assert.assertEquals(expected, result);

            }
        }
    }

    @RunWith(value = Parameterized.class)
    public static class GetDistanceTest {

        private Node node1;
        private Node node2;
        private boolean expectException;
        private Class<? extends Exception> expectedExceptionType;

        private NetworkTopologyImpl sut;


        public GetDistanceTest(Node node1, Node node2, boolean expectException, Class<? extends Exception> expectedExceptionType) {
            this.node1 = node1;
            this.node2 = node2;
            this.expectException = expectException;
            this.expectedExceptionType = expectedExceptionType;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {

                    {new NodeBase("127.0.0.1:4001", "/root/rack1"),new NodeBase("127.0.0.1:4001", "/root/rack1"), false, null},
                    {new NodeBase("127.0.0.1:4001", "/root/rack1"),new NodeBase("127.0.0.1:4002", "/root/rack2"), false, null},
                    {new NodeBase("127.0.0.1:4001", "/root/rack1"),new NodeBase("127.0.0.1:4002", "/root/rack1"), false, null},
                    //{null, null, true, NullPointerException.class},
                    {null,new NodeBase("127.0.0.1:4002", "/root/rack2"),true,NullPointerException.class},
                    {new NodeBase("127.0.0.1:4002", "/root/rack2"),null,true,NullPointerException.class}

            });
        }

        // Setup prima di ogni test
        @Before
        public void setUp() {
            sut = new NetworkTopologyImpl();
        }

        private int calculateExpectedDistance(Node node1, Node node2){

            System.out.println("Comparing nodes:");
            System.out.println("Node 1 address: " + node1.getName() + ", location: " + node1.getNetworkLocation());
            System.out.println("Node 2 address: " + node2.getName() + ", location: " + node2.getNetworkLocation());

            if(node1 == null || node2 == null){
                return Integer.MAX_VALUE;
            }

            if(node1.equals(node2)){
                return 0;
            }

            Node parent1 = node1.getParent();
            Node parent2 = node2.getParent();
            int distance = 0;

            while(parent1 != null && parent2 != null && parent1 != parent2) {

                parent1 = parent1.getParent();
                parent2 = parent2.getParent();
                distance += 2;
            }

            if(parent1 == parent2) {
                distance += 2; //
            }

            return distance;
        }
        @Test
        public void testGetDistance() {
            sut.add(node1);
            sut.add(node2);

            if (expectException) {
                assertThrows(expectedExceptionType, () -> {
                    sut.getDistance(node1,node2);
                });
            } else {
                int result = sut.getDistance(node1,node2);
                int expected = calculateExpectedDistance(node1,node2);

                // Stampare anche il risultato previsto e ottenuto
                System.out.println("Expected result: " + expected);
                System.out.println("Actual result: " + result);

                Assert.assertEquals(expected,result);

            }
        }
    }

}