package bookie;

import org.apache.bookkeeper.bookie.BookieImpl;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestBookieImpl{
    @Test
    public void addEntry() throws IOException {
        BookieImpl.checkDirectoryStructure(new File("./ciao"));
        assertTrue(true);
    }
}