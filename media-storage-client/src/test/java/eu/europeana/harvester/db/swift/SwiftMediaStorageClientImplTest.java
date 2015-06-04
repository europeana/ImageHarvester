package eu.europeana.harvester.db.swift;

import eu.europeana.harvester.domain.MediaFile;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.jclouds.ContextBuilder;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.joda.time.DateTime;
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

    private SwiftConfiguration swiftConfiguration;

    private SwiftMediaStorageClientImpl mediaStorageClient;

    private ObjectApi objectApi;
    private SwiftApi  swiftApi;
    private ContainerApi containerApi;

    private final String contentString = "id1-content-stuff";

    private final MediaFile mediaFile = new MediaFile("s", "id1", Collections.<String>emptyList(), "id1",
                                                      "id1.com", DateTime.now(), contentString.getBytes(), 1, "",
                                                      Collections.<String, String>emptyMap(), contentString.getBytes().length);

    @Before
    public void setUp() {
        swiftConfiguration = new SwiftConfiguration("https://auth.hydranodes.de:5000/v2.0",
                                                    "d35f3a21-cf35-48a0-a035-99bfc2252528.swift.tenant@a9s.eu",
                                                    "c9b9ddb5-4f64-4e08-9237-1d6848973ee1.swift.user@a9s.eu",
                                                    "78ae7i9XO3O7CcdkDa87", containerName, "hydranodes");
        mediaStorageClient = new SwiftMediaStorageClientImpl(swiftConfiguration);

        swiftApi = ContextBuilder.newBuilder("openstack-swift")
                                 .credentials(swiftConfiguration.getIdentity(), swiftConfiguration.getPassword())
                                 .endpoint(swiftConfiguration.getAuthUrl()).buildApi(SwiftApi.class);

        containerApi = swiftApi.getContainerApi(swiftConfiguration.getRegionName());
        objectApi = swiftApi.getObjectApi(swiftConfiguration.getRegionName(), swiftConfiguration.getContainerName());

        if (null == containerApi.get(swiftConfiguration.getContainerName())) {
            assertTrue(containerApi.create(swiftConfiguration.getContainerName()));

        }
    }

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
        assertTrue(EqualsBuilder.reflectionEquals(newMediaFile, retrievedFile));
    }
}
