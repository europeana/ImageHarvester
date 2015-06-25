package eu.europeana.harvester.db.swift;

import eu.europeana.harvester.domain.MediaFile;
import org.jclouds.ContextBuilder;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.io.IOException;

import static org.jclouds.io.Payloads.newByteArrayPayload;
import static org.junit.Assert.*;

/**
 * Created by salexandru on 03.06.2015.
 *
 * This tests represent a philosophical statement.  Do not trust an implementation, not even yours.
 * “A good programmer is someone who always looks both ways before crossing a one-way street.” (Doug Linder)
 */
@Ignore
public class SwiftMediaStorageClientImplTest {
    private static final String containerName = "swiftUnitTesting";

    private SwiftConfiguration swiftConfiguration;

    private SwiftMediaStorageClientImpl mediaStorageClient;

    private ObjectApi objectApi;
    private SwiftApi  swiftApi;
    private ContainerApi containerApi;

    private final String contentString = "id1-content-stuff";

    private final MediaFile mediaFile = new MediaFile("id1", null, "", null, null, null, null, contentString.getBytes(),
                                                      null, "", null, contentString.getBytes().length
                                                    );

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

    @After
    public void tearDown() {
        for (final SwiftObject swiftObject: objectApi.list()) {
            objectApi.delete(swiftObject.getName());
        }
        containerApi.deleteIfEmpty(containerName);
    }

    @Test
    public void test_CheckIfExists_False() {
        assertFalse (mediaStorageClient.checkIfExists("ana are mere"));
    }

    @Test
    public void test_CheckIfExists_True() {
        objectApi.put(mediaFile.getId(), newByteArrayPayload(mediaFile.getContent()));

        assertTrue(mediaStorageClient.checkIfExists(mediaFile.getId()));
    }

    @Test
    public void test_Delete_DoesNotExist() {
        assertFalse (mediaStorageClient.checkIfExists("ana are mere"));
        mediaStorageClient.delete("ana are mere");
        assertFalse (mediaStorageClient.checkIfExists("ana are mere"));
    }

    @Test
    public void test_Delete_DoesExist() {
        objectApi.put(mediaFile.getId(), newByteArrayPayload(mediaFile.getContent()));

        mediaStorageClient.delete(mediaFile.getId());
        assertFalse(mediaStorageClient.checkIfExists(mediaFile.getId()));
    }

    @Test
    public void test_CreateNewFile() {
        mediaStorageClient.createOrModify(mediaFile);
        assertTrue (mediaStorageClient.checkIfExists(mediaFile.getId()));
    }

    @Test
    public void test_RetrieveFile_DoesNotExist() throws IOException {
        assertNull (mediaStorageClient.retrieve(mediaFile.getId(), false));
    }


    @Test
    public void test_RetrieveFile_WithContent_DoesExist() throws IOException {
        mediaStorageClient.createOrModify(mediaFile);
        assertNotNull(mediaStorageClient.retrieve(mediaFile.getId(), false));
        final MediaFile retrievedFile = mediaStorageClient.retrieve(mediaFile.getId(), true);

        assertEquals(mediaFile.getId(), retrievedFile.getId());
        assertEquals(mediaFile.getSize(), retrievedFile.getSize());
        assertEquals (mediaFile.getContentType(), retrievedFile.getContentType());
        assertArrayEquals(mediaFile.getContent(), retrievedFile.getContent());
    }

    @Test
    public void test_RetrieveFile_WithoutContent_DoesExist() throws IOException {
        mediaStorageClient.createOrModify(mediaFile);
        assertNotNull(mediaStorageClient.retrieve(mediaFile.getId(), false));
        final MediaFile retrievedFile = mediaStorageClient.retrieve(mediaFile.getId(), true);

        assertEquals(mediaFile.getId(), retrievedFile.getId());
    }

    @Test
    public void test_ModifyFile() throws IOException {
        mediaStorageClient.createOrModify(mediaFile);

        final MediaFile newMediaFile = new MediaFile("id1", null, "", null, null, null, null, new byte[] {1, 2},
                                                     null, "", null, contentString.getBytes().length
        );

        mediaStorageClient.createOrModify(newMediaFile);

        final MediaFile retrievedFile = mediaStorageClient.retrieve(newMediaFile.getId(), true);

        assertEquals (newMediaFile.getId(), retrievedFile.getId());
        assertEquals (newMediaFile.getSize(), retrievedFile.getSize());
        assertEquals(newMediaFile.getContentType(), retrievedFile.getContentType());
        assertArrayEquals(newMediaFile.getContent(), retrievedFile.getContent());
    }
}
