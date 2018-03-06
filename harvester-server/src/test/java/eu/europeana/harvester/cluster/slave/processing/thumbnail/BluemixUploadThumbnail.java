/*
 * Copyright 2007-2018 The Europeana Foundation
 *
 *  Licenced under the EUPL, Version 1.1 (the "Licence") and subsequent versions as approved
 *  by the European Commission;
 *  You may not use this work except in compliance with the Licence.
 *
 *  You may obtain a copy of the Licence at:
 *  http://joinup.ec.europa.eu/software/page/eupl
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under
 *  the Licence is distributed on an "AS IS" basis, without warranties or conditions of
 *  any kind, either express or implied.
 *  See the Licence for the specific language governing permissions and limitations under
 *  the Licence.
 */

package eu.europeana.harvester.cluster.slave.processing.thumbnail;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.db.s3.BluemixConfiguration;
import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.ThumbnailType;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import static eu.europeana.harvester.TestUtils.Image4;
import static eu.europeana.harvester.TestUtils.PATH_COLORMAP;
import static eu.europeana.harvester.TestUtils.getPath;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import eu.europeana.harvester.db.s3.S3MediaClientStorage;
import static eu.europeana.harvester.TestUtils.*;

/**
 * This class uploads a single thumbnail to Bluemix S3 that is also available on Amazon S3, in order to test the
 * Search API's Thumbnailcontroller
 * <p>
 * Created by luthien on 5/03/2018.
 */
public class BluemixUploadThumbnail {

    private static Config config;
    private static S3MediaClientStorage s3Client;
    private static MediaFile uploadThumbnail;
    // set to false to keep thumbnail
    private boolean deleteAgain = true;

    @BeforeClass
    public static void initClientAndTestServer() throws IOException {
        final File configFile = new File("../src/test/resources/master-slave-configs/scenario1/slave.conf");
        if (!configFile.exists()) {
            System.out.println("CLUSTER Slave Config file not found!");
            System.exit(-1);
        }
        config = ConfigFactory.parseFileAnySyntax(configFile, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));
        s3Client = new S3MediaClientStorage(BluemixConfiguration.valueOf(config.getConfig("media-storage")));
    }

    /**
     * Check if our default test object already exists, if so it's probably from a previous test so we delete it
     */
    private void deleteTestObject(String id) {
        if (s3Client.checkIfExists(id)) {
            try {
                s3Client.delete(id);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void uploadThumbnail() throws IOException, NoSuchAlgorithmException {
        System.out.println("Starting simple test putting and retrieving two thumbnails");
        System.out.println("Creating the thumbnail");
        try {
            uploadThumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.IMAGE, PATH_COLORMAP)
                                                       .createMediaFileWithThumbnail(ThumbnailType.MEDIUM.getWidth(),
                                                                                     ThumbnailType.MEDIUM.getHeight(),
                                                                                     "",
                                                                                     "http://ogimages.bl.uk/images/015/015HZZ00001494BU00007001%5BSVC1%5D.jpg",
                                                                                     Files.toByteArray(new File(getPath(Image4))),
                                                                                     getPath(Image4));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Upload thumbnail with ID: " + uploadThumbnail.getId());
        // 200 x 200 thumbnail
        s3Client.createOrModifyNoDel(uploadThumbnail);
        System.out.println("Retrieve thumbnail for verification");
        MediaFile thumbnailRetrieved = s3Client.retrieve(uploadThumbnail.getId(), true);

        assertEquals(thumbnailRetrieved.getContent().length, uploadThumbnail.getContent().length);
        assertEquals(new String(thumbnailRetrieved.getContent()), new String(uploadThumbnail.getContent()));

        if (deleteAgain){
            System.out.println("Delete thumbnail on S3 server");
            deleteTestObject(uploadThumbnail.getId());
            assertFalse(s3Client.checkIfExists(uploadThumbnail.getId()));
        } else {
            System.out.println("Leave thumbnail on S3 server");
            assertTrue(s3Client.checkIfExists(uploadThumbnail.getId()));
        }
    }

}
