package eu.europeana.harvester.httpclient.response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import static org.junit.Assert.*;

public class HttpRetrieveResponseDiskStorageTests {

    private static final String PATH_PREFIX = Paths.get("harvester-server/src/test/resources/").toAbsolutePath().toString() + "/";
    private static final String filePath = PATH_PREFIX+"file_storage.txt";

    @Before
    public void setUp() {
    }

    @Test
    public void canStoreDataCorrectly() throws Exception {
        final HttpRetrieveResponseDiskStorage storage = new HttpRetrieveResponseDiskStorage(filePath);
        assertEquals(storage.getContentSizeInBytes().intValue(),0);
        storage.addContent("123".getBytes());
        assertEquals(storage.getContentSizeInBytes().intValue(), 3);
        storage.addContent("456".getBytes());
        assertEquals(storage.getContentSizeInBytes().intValue(), 6);
        storage.close();
        assertEquals(storage.getContentSizeInBytes().intValue(), 6);
    }


    @After
    public void tearDown() throws IOException {
        Files.delete(Paths.get(filePath));
    }

}
