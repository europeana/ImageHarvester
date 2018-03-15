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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.db.s3.BluemixConfiguration;
import eu.europeana.harvester.db.s3.S3Configuration;
import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.ThumbnailType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import static eu.europeana.harvester.TestUtils.Image1;
import static eu.europeana.harvester.TestUtils.PATH_COLORMAP;
import static eu.europeana.harvester.TestUtils.getPath;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import eu.europeana.harvester.db.s3.S3MediaClientStorage;
import static eu.europeana.harvester.TestUtils.*;

/**
 * This class tests object storage and retrieval at Bluemix S3.
 * <p>
 * Adapted from ObjectStorage.S3ObjectStorageClientTest (Patrick Ehlert)
 * Created by luthien on 21/02/2018.
 */
public class S3ObjectStorageClientTest {

    private static Config config;

    private static final Logger LOG = LogManager.getLogger(S3ObjectStorageClientTest.class.getName());
    private static boolean isBluemix = true; // for now set to true

    private static final int STRESS_TEST_SIZE = 5;

    private static S3MediaClientStorage s3Client;


    private static long start;
    private static int nrItems = 0;
    private static long timingPutContent = 0;
    private static long timingRetrieveContent = 0;
    private static long timingDeleteContent = 0;

    private static MediaFile mediumThumbnail;
    private static MediaFile largeThumbnail;

    @BeforeClass
    public static void initClientAndTestServer() throws IOException {
        final File configFile = new File("../src/test/resources/master-slave-configs/scenario1/slave.conf");
        if (!configFile.exists()) {
            System.out.println("CLUSTER Slave Config file not found!");
            System.exit(-1);
        }
        config = ConfigFactory.parseFileAnySyntax(configFile, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));
        if (isBluemix) {
            s3Client = new S3MediaClientStorage(BluemixConfiguration.valueOf(config.getConfig("media-storage")));
        } else {
            s3Client = new S3MediaClientStorage(S3Configuration.valueOf(config.getConfig("media-storage")));
        }
        initiateThumbNails();
    }

    @Test
    public void testSimplePutAndRetrieval() throws IOException, NoSuchAlgorithmException {
        System.out.println("Starting simple test putting and retrieving two thumbnails");
        // 200 x 200 thumbnail
        storeAndRetrieveThumbnail(mediumThumbnail);
        // 400 x 400 thumbnail
        storeAndRetrieveThumbnail(largeThumbnail);
        System.out.println("... simple test done");
        printTimings();
    }


    /**
     * Does a stress test of putting, retrieving and deleting a small test object.
     * Note that this may take a while (approx. 4 minutes for 1000 items)
     */
    @Test (timeout=500000)
    public void testStressPutAndRetrieval() throws IOException, NoSuchAlgorithmException {
        System.out.println("Starting stress test with size " + STRESS_TEST_SIZE);
        for (int i = 0; i < STRESS_TEST_SIZE ; i++) {
            // alternate between medium and small thumbnails
            if (i % 2 == 0) {
                storeAndRetrieveThumbnail(mediumThumbnail);
            } else {
                storeAndRetrieveThumbnail(largeThumbnail);
            }
            // provide feedback about progress
            if (i % (Math.round(STRESS_TEST_SIZE / 5.0)) == 0) {
                System.out.println(String.format("%.1f", i * 100.0 / STRESS_TEST_SIZE) +"%");
            }
        }
        System.out.println("... stress test done");
        printTimings();
    }

    private void storeAndRetrieveThumbnail(MediaFile thumbnailSent) throws IOException, NoSuchAlgorithmException {

        start = System.nanoTime();
        s3Client.createOrModifyNoDel(thumbnailSent);
        timingPutContent += (System.nanoTime() - start);

        start = System.nanoTime();
        MediaFile thumbnailRetrieved = s3Client.retrieve(thumbnailSent.getId(), true);
        timingRetrieveContent += (System.nanoTime() - start);

        assertEquals(thumbnailRetrieved.getContent().length, thumbnailSent.getContent().length);
        assertEquals(new String(thumbnailRetrieved.getContent()), new String(thumbnailSent.getContent()));

        start = System.nanoTime();
        deleteTestObject(thumbnailSent.getId());
        timingDeleteContent += (System.nanoTime() - start);

        assertFalse(s3Client.checkIfExists(thumbnailSent.getId()));
        nrItems++;
    }

    @Test
    public void testWithExistingThumbnail() throws IOException, NoSuchAlgorithmException {
        System.out.println("Starting test uploading the same thumbnail twice");

        // first time
        s3Client.createOrModifyNoDel(mediumThumbnail);
        MediaFile thumbnailRetrieved = s3Client.retrieve(mediumThumbnail.getId(), true);
        assertEquals(thumbnailRetrieved.getContent().length, mediumThumbnail.getContent().length);
        assertEquals(new String(thumbnailRetrieved.getContent()), new String(mediumThumbnail.getContent()));

        s3Client.createOrModifyNoDel(mediumThumbnail);
        MediaFile thumbnailRetrievedTwice = s3Client.retrieve(mediumThumbnail.getId(), true);
        assertEquals(thumbnailRetrievedTwice.getContent().length, mediumThumbnail.getContent().length);
        assertEquals(new String(thumbnailRetrievedTwice.getContent()), new String(mediumThumbnail.getContent()));

        deleteTestObject(mediumThumbnail.getId());
        System.out.println("... existing thumbnail test done");
    }

    private static void initiateThumbNails() {
        try {
            mediumThumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.IMAGE, PATH_COLORMAP)
                                                       .createMediaFileWithThumbnail(ThumbnailType.MEDIUM.getWidth(),
                                                                                     ThumbnailType.MEDIUM.getHeight(),
                                                                                     "",
                                                                                     getPath(Image1),
                                                                                     filesInBytes.get(Image1),
                                                                                     getPath(Image1));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            largeThumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.IMAGE, PATH_COLORMAP)
                                                       .createMediaFileWithThumbnail(ThumbnailType.LARGE.getWidth(),
                                                                                     ThumbnailType.LARGE.getHeight(),
                                                                                     "",
                                                                                     getPath(Image1),
                                                                                     filesInBytes.get(Image1),
                                                                                     getPath(Image1));
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    private static void printTimings() {
        System.out.println("Time spend on retrieval of " + nrItems + " items...");

        System.out.println("  ... storing thumbnails    : " + timingPutContent/1000000 + " ms | " +
                           (timingPutContent/1000000) / nrItems + " ms / object" );
        System.out.println("  ... retrieving thumbnails : " + timingRetrieveContent/1000000 + " ms | " +
                           (timingRetrieveContent/1000000) / nrItems + " ms / object" );
        System.out.println("  ... deleting thumbnails   : " + timingDeleteContent/1000000 + " ms | " +
                           (timingDeleteContent/1000000) / nrItems + " ms / object" );
        nrItems = 0;
        timingPutContent = 0;
        timingRetrieveContent = 0;
        timingDeleteContent = 0;
    }

}
