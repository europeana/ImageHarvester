package eu.europeana.harvester.db.filesystem;

import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class FileSystemMediaStorageClientImplTest {

    private static final String PATH_PREFIX = Paths.get("media-storage-client/src/test/resources/filesystem").toAbsolutePath().toString() + "/";

    private static final MediaStorageClient client = new FileSystemMediaStorageClientImpl(PATH_PREFIX);

    private final String contentString = "id1-content-stuff";

    private final MediaFile mediaFile;

    @Before
    public void setUp() throws IOException {
        FileUtils.forceMkdir(Paths.get(PATH_PREFIX).toFile());
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.forceDelete(Paths.get(PATH_PREFIX).toFile());
    }

    public FileSystemMediaStorageClientImplTest() throws NoSuchAlgorithmException {
        mediaFile = new MediaFile("s", "id1", Collections.<String>emptyList(), "id1",
        "id1.com", DateTime.now(), contentString.getBytes(), 1, "",
        Collections.<String, String>emptyMap(), contentString.getBytes().length);
    }

    @Test
    public void canCreateAndRetrieveMedia() throws IOException, NoSuchAlgorithmException {
        client.createOrModify(mediaFile);
        MediaFile retrievedFile = client.retrieve(mediaFile.getId(),true);
        assertEquals(retrievedFile.getContent().length,mediaFile.getContent().length);
        assertEquals(new String(retrievedFile.getContent()),new String(mediaFile.getContent()));
        client.delete(mediaFile.getId());
    }



}
