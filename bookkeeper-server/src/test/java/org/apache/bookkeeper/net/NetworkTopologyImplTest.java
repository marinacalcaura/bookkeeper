package org.apache.bookkeeper.net;

import org.junit.*;
import static org.junit.Assert.*;

import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;


// Class for the test
@RunWith(value = Enclosed.class)
public class NetworkTopologyImplTest {

    @RunWith(value = Parameterized.class)
    public static class AddNodeTest{

        private Node node;
        private boolean expectException;
        private Class<? extends Exception> expectedExceptionType;

        private NetworkTopologyImpl sut; // SUT = System Under Test

        // Constructor for parameterized test
        public AddNodeTest(Node node, boolean expectException, Class<? extends Exception> expectedExceptionType) {
            this.node = node;
            this.expectException = expectException;
            this.expectedExceptionType = expectedExceptionType;
        }

        // Define the test parameters (inputs)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {

                    {new NodeBase("127.0.0.1:5555", "/root"), false, null},  // LeafNode, valid case, no exception
                    {new NetworkTopologyImpl.InnerNode("127.0.0.1:4001", "/root/inner"), true, IllegalArgumentException.class }, // InnerNode, should throw IllegalArgumentException
                    {null, false, null},  // null node, should just return (no exception)
                    //{new NodeBase("127.0.0.1:4002", "/root/127.0.0.1:3999"), true, IllegalArgumentException.class},  // Invalid topology node, should throw InvalidTopologyException
                    //alla seconda iterazione
                    {new NodeBase("127.0.0.1:4002", "/root/first"), true, NetworkTopologyImpl.InvalidTopologyException.class },  // Invalid topology node, should throw InvalidTopologyException

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
                // In case we don't expect an exception, handle the `null` case inside this block
                if (node == null) {
                    sut.add(node);  // Add a null node, should not throw an exception
                    assertFalse(sut.contains(node));  // The node should not be contained (as it's null)
                } else {
                    sut.add(node);  // Add the non-null node
                    assertTrue(sut.contains(node));  // Verify that the node has been added successfully
                }
            }
        }
    }

    @RunWith(value = Parameterized.class)
    public static class RemoveNodeTest {

        private Node node;
        private boolean expectException;
        private Class<? extends Exception> expectedExceptionType;

        private NetworkTopologyImpl sut; // SUT = System Under Test

        // Constructor for parameterized test
        public RemoveNodeTest(Node node, boolean expectException, Class<? extends Exception> expectedExceptionType) {
            this.node = node;
            this.expectException = expectException;
            this.expectedExceptionType = expectedExceptionType;
        }

        // Define the test parameters (inputs)
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {

                    {new NodeBase("127.0.0.1:5555", "/root"), false, null},  // LeafNode, valid case, no exception
                    {new NetworkTopologyImpl.InnerNode("127.0.0.1:5556", "/root/inner"), true, IllegalArgumentException.class }, // InnerNode, should throw IllegalArgumentException
                    {null, false, null},  // null node, should just return (no exception)

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
}