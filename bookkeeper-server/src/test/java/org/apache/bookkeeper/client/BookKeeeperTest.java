package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


@RunWith(value = Enclosed.class)
public class BookKeeeperTest {
    @RunWith(value = Parameterized.class)
    public static class CreateLedgerValidTest {

        private LocalBookKeeper bookKeeper;
        private BookKeeper client;
        private int ensembleSize;
        private int writeQuorumSize;
        private int ackQuorumSize;
        private BookKeeper.DigestType digestType;
        private byte[] passwd;
        private Map<String,byte[]> customMetadata;

        public CreateLedgerValidTest(int ensembleSize, int writeQuorumSize, int ackQuorumSize,BookKeeper.DigestType digestType, byte[] passwd, Map<String,byte[]> customMetadata)  {
            this.ensembleSize=ensembleSize;
            this.writeQuorumSize=writeQuorumSize;
            this.ackQuorumSize=ackQuorumSize;
            this.digestType=digestType;
            this.passwd=passwd;
            this.customMetadata=customMetadata;
        }
        @Parameterized.Parameters
        public static Collection configure(){
            Map<String, byte[]> nonEmptyMetadata = new HashMap<>();
            nonEmptyMetadata.put("myMetadata", "MyCustomMetadata".getBytes());

            return Arrays.asList(new Object[][] {

                    {3, 2, 1, BookKeeper.DigestType.MAC, new byte[6], nonEmptyMetadata},
                    {2, 2, 1, BookKeeper.DigestType.CRC32, new byte[6], null},


                    {2, 1, 1, BookKeeper.DigestType.MAC, new byte[6], null},
                    {1, 1, 1, BookKeeper.DigestType.CRC32, new byte[0], new HashMap<String, byte[]>()},

                    {2, 1, 0, BookKeeper.DigestType.DUMMY, new byte[0], new HashMap<String, byte[]>()},
                    {1, 1, 0, BookKeeper.DigestType.MAC, new byte[6], null},

                    {1, 0, 0, BookKeeper.DigestType.MAC, new byte[6], null},
                    {0, 0, 0, BookKeeper.DigestType.DUMMY, new byte[6], nonEmptyMetadata},

                    {1, 0, -1, BookKeeper.DigestType.MAC, new byte[6], null},
                    {0, 0, -1, BookKeeper.DigestType.CRC32C, new byte[6], nonEmptyMetadata},

                    {0, -1, -1, BookKeeper.DigestType.MAC, new byte[6], null},


            });
        }


        @Before
        public void startServer() throws Exception {
            ServerConfiguration configuration = new ServerConfiguration();
            configuration.setAllowLoopback(true);
            bookKeeper = LocalBookKeeper.getLocalBookies("127.0.0.1", 34567, 3, true, configuration);
            bookKeeper.start();
            client = new BookKeeper("127.0.0.1:34567");
        }

        @After
        public void closeServer() throws Exception {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    System.err.println("Failed to close BookKeeper client: " + e.getMessage());
                }
            }
            if (bookKeeper != null) {
                try {
                    bookKeeper.close();
                } catch (Exception e) {
                    System.err.println("Failed to close LocalBookKeeper: " + e.getMessage());
                }
            }
            Thread.sleep(500);
        }

        /*Il test verifica la corretta creazione di un ledger utilizzando i parametri forniti,
        controllando che i metadati del ledger creato siano consistenti con i parametri di input.
        */

        @Test
        public void shouldCreateValidLedger() throws BKException, InterruptedException {

            LedgerHandle createdLedger = client.createLedger(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
            LedgerMetadata ledgerMetadata = createdLedger.getLedgerMetadata();

            //verifico se la password nel ledger corrisponde a quella fornita
            boolean isPasswordValid = ledgerMetadata.getPassword().length == passwd.length;
            for (int index = 0; index < passwd.length; index++) {
                if (ledgerMetadata.getPassword()[index] != passwd[index]) {
                    isPasswordValid = false;
                    break;
                }
            }

            //confronta i metadati personalizzati, se presenti
            boolean areCustomMetadataValid = (customMetadata == null || ledgerMetadata.getCustomMetadata().equals(customMetadata));

            //verifica che i parametri dell'ledger siano corretti
            boolean isLedgerValid = ledgerMetadata.getEnsembleSize() == ensembleSize
                    && ledgerMetadata.getWriteQuorumSize() == writeQuorumSize
                    && ledgerMetadata.getAckQuorumSize() == ackQuorumSize
                    && isPasswordValid
                    && areCustomMetadataValid;
            Assert.assertTrue("Ledger creation failed due to invalid parameters", isLedgerValid);
        }

    }

    @RunWith(value = Parameterized.class)
    public static class CreateLedgerInvalidTest {


        private LocalBookKeeper bookKeeper;
        private BookKeeper client;
        private int ensembleSize;
        private int writeQuorumSize;
        private int ackQuorumSize;
        private BookKeeper.DigestType digestType;
        private byte[] passwd;
        private Map<String,byte[]> customMetadata;

        public CreateLedgerInvalidTest(int ensembleSize, int writeQuorumSize, int ackQuorumSize,BookKeeper.DigestType digestType, byte[] passwd, Map<String,byte[]> customMetadata)  {
            this.ensembleSize=ensembleSize;
            this.writeQuorumSize=writeQuorumSize;
            this.ackQuorumSize=ackQuorumSize;
            this.digestType=digestType;
            this.passwd=passwd;
            this.customMetadata=customMetadata;
        }

        @Parameterized.Parameters
        public static Collection configure(){
            return Arrays.asList(new Object[][] {
                    //{1, 2, 1, BookKeeper.DigestType.MAC, new byte[6], null}
                    //{0, 1, 1, BookKeeper.DigestType.DUMMY, new byte[6], null},
                    {1, 0, 1, BookKeeper.DigestType.MAC, new byte[6], null},
                    {0, 0, 1, BookKeeper.DigestType.CRC32, new byte[6], null},
                    //{-1, 0, 1, BookKeeper.DigestType.MAC, new byte[6], null},
                    //{0, 1, 0, BookKeeper.DigestType.CRC32C, new byte[6], null},
                    //{-1, 0, 0, BookKeeper.DigestType.MAC, new byte[0], null},
                    {0, -1, 0, BookKeeper.DigestType.DUMMY, new byte[6], null},
                    {-1, -1, 0, BookKeeper.DigestType.CRC32C, new byte[6], null},
                    //{-2, -1, 0, BookKeeper.DigestType.MAC, new byte[0], null},
                    //{-1, 0, -1, BookKeeper.DigestType.CRC32, new byte[6], null},
                    {-1, -1, -1, BookKeeper.DigestType.MAC, new byte[0], null},
                    //{-2, -1, -1, BookKeeper.DigestType.DUMMY, new byte[6], null},
                    {-1, -2, -1, BookKeeper.DigestType.MAC, new byte[6], null},
                    {-2, -2, -1, BookKeeper.DigestType.MAC, new byte[6], new HashMap<String, byte[]>()},
                    // {-3, -2, -1, BookKeeper.DigestType.CRC32, new byte[6], null}
            });
        }
        @Before
        public void startServer() throws Exception {
            ServerConfiguration configuration = new ServerConfiguration();
            configuration.setAllowLoopback(true);
            bookKeeper = LocalBookKeeper.getLocalBookies("127.0.0.1", 34567, 3, true, configuration);
            bookKeeper.start();
            client = new BookKeeper("127.0.0.1:34567");
        }

        @After
        public void closeServer() throws Exception {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    System.err.println("Failed to close BookKeeper client: " + e.getMessage());
                }
            }
            if (bookKeeper != null) {
                try {
                    bookKeeper.close();
                } catch (Exception e) {
                    System.err.println("Failed to close LocalBookKeeper: " + e.getMessage());
                }
            }
            Thread.sleep(500);
        }

        /*verifica se il metodo createLedger lancia un'eccezione quando viene
        invocato con parametri non validi
         */
        @Test
        public void testCreateLedgerException() throws BKException, InterruptedException {
            boolean exceptionThrown = false;
            try {
                client.createLedger(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
            } catch (Exception e) {
                exceptionThrown = true;
            }
            Assert.assertTrue(exceptionThrown);
        }

    }

    @RunWith(value = Parameterized.class)
    public static class OpenLedgerTest {


        private LocalBookKeeper bookKeeper;
        private BookKeeper client;
        private BookKeeper.DigestType digestType;
        private byte[] passwd;

        public OpenLedgerTest(BookKeeper.DigestType digestType, byte[] passwd) {
            this.digestType = digestType;
            this.passwd = passwd;
        }

        @Parameterized.Parameters
        public static Collection configure() {
            return Arrays.asList(new Object[][]{
                    {BookKeeper.DigestType.MAC, "test_pwd".getBytes()},
                    {BookKeeper.DigestType.CRC32, new byte[]{}}
            });
        }

        @Before
        public void startServer() throws Exception {
            ServerConfiguration configuration = new ServerConfiguration();
            configuration.setAllowLoopback(true);
            bookKeeper = LocalBookKeeper.getLocalBookies("127.0.0.1", 34567, 3, true, configuration);
            bookKeeper.start();
            client = new BookKeeper("127.0.0.1:34567");
        }

        @After
        public void closeServer() throws Exception {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    System.err.println("Failed to close BookKeeper client: " + e.getMessage());
                }
            }
            if (bookKeeper != null) {
                try {
                    bookKeeper.close();
                } catch (Exception e) {
                    System.err.println("Failed to close LocalBookKeeper: " + e.getMessage());
                }
            }
            Thread.sleep(500);
        }


        @Test
        public void testOpenLedger() throws BKException, InterruptedException {

            LedgerHandle handle = client.createLedger(this.digestType, this.passwd);

            String testEntry = "test_pwd";
            byte[] entryValue = testEntry.getBytes(StandardCharsets.UTF_8);

            handle.addEntry(entryValue);

            long ledgerId = handle.getId();
            handle.close();

            LedgerHandle openedHandle = client.openLedger(ledgerId, this.digestType, this.passwd);

            try {
                LedgerEntry entry = openedHandle.readLastEntry();
                byte[] actualEntry = entry.getEntry();

                Assert.assertArrayEquals("The ledger entry is not as expected", entryValue, actualEntry);
            } catch (Exception e) {
                Assert.fail("Failed to read the expected entry from the ledger: " + e.getMessage());
            }
        }
    }

    @RunWith(value = Parameterized.class)
    public static class OpenLedgerExcTest {


        private LocalBookKeeper bookKeeper;
        private BookKeeper client;
        private BookKeeper.DigestType digestType;
        private byte[] passwd;

        public OpenLedgerExcTest(BookKeeper.DigestType digestType, byte[] passwd) {
            this.digestType = digestType;
            this.passwd = passwd;
        }

        @Parameterized.Parameters
        public static Collection configure() {
            return Arrays.asList(new Object[][]{
                    {BookKeeper.DigestType.CRC32C, "test_pwd".getBytes()},
                    {BookKeeper.DigestType.MAC, new byte[]{}},
            });
        }

        @Before
        public void startServer() throws Exception {
            ServerConfiguration configuration = new ServerConfiguration();
            configuration.setAllowLoopback(true);
            bookKeeper = LocalBookKeeper.getLocalBookies("127.0.0.1", 34567, 3, true, configuration);
            bookKeeper.start();
            client = new BookKeeper("127.0.0.1:34567");
        }

        @After
        public void closeServer() throws Exception {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    System.err.println("Failed to close BookKeeper client: " + e.getMessage());
                }
            }
            if (bookKeeper != null) {
                try {
                    bookKeeper.close();
                } catch (Exception e) {
                    System.err.println("Failed to close LocalBookKeeper: " + e.getMessage());
                }
            }
            Thread.sleep(500);
        }


        @Test
        public void testOpenLedgerBadPassword() throws BKException, InterruptedException {
            // Caso di password errata
            LedgerHandle handle = client.createLedger(this.digestType, "bad_pwd".getBytes(StandardCharsets.UTF_8));

            try {
                client.openLedger(handle.getId(), this.digestType, this.passwd);
                Assert.fail("Expected BKException due to bad password");
            } catch (BKException e) {
                Assert.assertTrue("BKException is expected due to wrong password", true);
            }
        }

        @Test
        public void testOpenLedgerNonExistent() throws BKException, InterruptedException {
            // Caso di ledger inesistente
            try {

                client.openLedger(-1, this.digestType, this.passwd);
                Assert.fail("Expected BKException due to non-existent ledger");
            } catch (BKException e) {
                Assert.assertTrue("BKException is expected due to non-existent ledger", true);
            }
        }

    }


    /*@RunWith(value = Parameterized.class)
    public static class DeleteLedgerTest {


        private LocalBookKeeper bookKeeper;
        private BookKeeper client;
        private int ensSize;
        private int writeQuorumSize;
        private int ackQuorumSize;
        private BookKeeper.DigestType digestType;
        private byte[] passwd;
        private Map<String, byte[]> customMetadata;

        public DeleteLedgerTest(int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, Map<String, byte[]> customMetadata) {
            this.ensSize = ensSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.digestType = digestType;
            this.passwd = passwd;
            this.customMetadata = customMetadata;
        }

        @Parameterized.Parameters
        public static Collection configure() {
            return Arrays.asList(new Object[][]{
                    {1, 0, 0, BookKeeper.DigestType.CRC32, "test".getBytes(), null}
            });
        }

        @Before
        public void startServer() throws Exception {
            ServerConfiguration configuration = new ServerConfiguration();
            configuration.setAllowLoopback(true);
            bookKeeper = LocalBookKeeper.getLocalBookies("127.0.0.1", 34567, 3, true, configuration);
            bookKeeper.start();
            client = new BookKeeper("127.0.0.1:34567");
        }

        @After
        public void closeServer() throws Exception {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    System.err.println("Failed to close BookKeeper client: " + e.getMessage());
                }
            }
            if (bookKeeper != null) {
                try {
                    bookKeeper.close();
                } catch (Exception e) {
                    System.err.println("Failed to close LocalBookKeeper: " + e.getMessage());
                }
            }
            Thread.sleep(500);
        }


        @Test
        public void testDeleteLedgerSuccess() throws Exception {
            LedgerHandle handle = client.createLedger(this.ensSize,this.writeQuorumSize,this.ackQuorumSize, BookKeeper.DigestType.CRC32C,this.passwd,this.customMetadata);
            long ledgerId = handle.getId();
            handle.close();

            try {
                client.deleteLedger(ledgerId);

                CompletableFuture<Versioned<LedgerMetadata>> future =
                        client.getLedgerManager().readLedgerMetadata(ledgerId);
                SyncCallbackUtils.waitForResult(future);
                Assert.fail("Expected BKNoSuchLedgerExistsOnMetadataServerException, but none was thrown.");
            } catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
                Assert.assertTrue("Ledger deletion succeeded as expected.", true);
            }
        }

        @Test
        public void testDeleteLedgerFailure() {
            long invalidLedgerId = -1; // Ledger ID non esistente
            try {
                client.deleteLedger(invalidLedgerId);
                Assert.fail("Expected BKException, but none was thrown.");
            } catch (BKException e) {
                Assert.assertTrue("Correct exception was thrown for invalid ledger deletion.", true);
            } catch (InterruptedException e) {
                Assert.fail("Unexpected InterruptedException.");
            }
        }

    }*/

}
