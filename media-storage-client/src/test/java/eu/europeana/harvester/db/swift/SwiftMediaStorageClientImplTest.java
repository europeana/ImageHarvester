package eu.europeana.harvester.db.swift;

import eu.europeana.harvester.domain.MediaFile;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by salexandru on 03.06.2015.
 */
public class SwiftMediaStorageClientImplTest {
    private static final String containerName = "swiftUnitTesting";
    private static final AccountConfig accountConfig = new AccountConfig();

    static {
        accountConfig.setUsername("c9b9ddb5-4f64-4e08-9237-1d6848973ee1.swift.user@a9s.eu");
        accountConfig.setPassword("78ae7i9XO3O7CcdkDa87");
        accountConfig.setAuthUrl("https://auth.hydranodes.de:5000/v2.0");
        accountConfig.setTenantId("3c678adbb69641018b645caa104b9252");
    }

    private SwiftMediaStorageClientImpl mediaStorageClient;

    private final String contentString = "id1-content-stuff";

    private final MediaFile mediaFile = new MediaFile("s", "id1", Collections.<String>emptyList(), "id1",
            "id1.com", DateTime.now(), contentString.getBytes(), 1, "",
            Collections.<String, String>emptyMap(), contentString.getBytes().length);

    @Before
    public void setUp() {
    /*
        final Container container = new AccountFactory(accountConfig).createAccount().getContainer(containerName);
        if (!container.exists()) container.create();
        mediaStorageClient = new SwiftMediaStorageClientImpl(accountConfig, containerName);
    */
    }

    @After
    public void tearDown() {
//        new AccountFactory(accountConfig).createAccount().getContainer(containerName).delete();
    }

    @Test
    public void test_CheckIfExists_False() {
        assertFalse (mediaStorageClient.checkIfExists("ana are mere"));
    }

    @Test
    public void test_CheckIfExists_True() {
        mediaStorageClient.createOrModify(mediaFile);
        assertTrue (mediaStorageClient.checkIfExists(mediaFile.getId()));
    }

    @Test
    public void test_Delete_DoesntExist() {
        assertFalse (mediaStorageClient.checkIfExists("ana are mere"));
        mediaStorageClient.delete("ana are mere");
        assertFalse (mediaStorageClient.checkIfExists("ana are mere"));
    }

    @Test
    public void test_Delete_DoesExist() {
        mediaStorageClient.createOrModify(mediaFile);
        assertTrue (mediaStorageClient.checkIfExists(mediaFile.getId()));
        mediaStorageClient.delete(mediaFile.getId());
        assertFalse(mediaStorageClient.checkIfExists(mediaFile.getId()));
    }

    @Test
    public void test_CreateFile() {
        mediaStorageClient.createOrModify(mediaFile);
        assertTrue (mediaStorageClient.checkIfExists(mediaFile.getId()));
    }

    @Test
    public void test_RetrieveFile_DoesntExist() throws IOException {
        assertNull (mediaStorageClient.retrieve(mediaFile.getId(), false));
    }


    @Test
         public void test_RetrieveFile_WithContent_DoesExist() throws IOException {
        mediaStorageClient.createOrModify(mediaFile);
        assertNotNull (mediaStorageClient.retrieve(mediaFile.getId(), false));
        final MediaFile retrievedFile = mediaStorageClient.retrieve(mediaFile.getId(), true);
        assertTrue (EqualsBuilder.reflectionEquals(mediaFile, retrievedFile));
    }

    @Test
    public void test_RetrieveFile_WithoutContent_DoesExist() throws IOException {
        mediaStorageClient.createOrModify(mediaFile);
        assertNotNull (mediaStorageClient.retrieve(mediaFile.getId(), false));
        final MediaFile retrievedFile = mediaStorageClient.retrieve(mediaFile.getId(), true);

        assertTrue (EqualsBuilder.reflectionEquals(mediaFile, retrievedFile, new String[] {"contentMd5", "content", "contentType"}));
        assertNull (retrievedFile.getContent());
        assertNull (retrievedFile.getContentMd5());
        assertNull (retrievedFile.getContentType());
    }

    @Test
    public void test_ModifyFile() throws IOException {
        mediaStorageClient.createOrModify(mediaFile);
        MediaFile newMediaFile = new MediaFile("s", "id1", Collections.<String>emptyList(), "id1",
                "id1.com", DateTime.now(), new byte[] {1}, 1, "",
                Collections.<String, String>emptyMap(), 1);
        mediaStorageClient.createOrModify(mediaFile);
        assertTrue (mediaStorageClient.checkIfExists(mediaFile.getId()));

        final MediaFile retrievedFile = mediaStorageClient.retrieve(newMediaFile.getId(), true);
        assertTrue (EqualsBuilder.reflectionEquals(newMediaFile, retrievedFile));
    }
}
