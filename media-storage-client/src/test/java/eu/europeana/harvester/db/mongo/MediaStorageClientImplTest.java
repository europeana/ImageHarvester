package eu.europeana.harvester.db.mongo;

import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.MediaStorageClientConfig;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.*;

public class MediaStorageClientImplTest {
//
//    private MediaStorageClientImpl client;
//
//    @Before
//    public void setup() throws IOException {
//        final String configFilePath = "./extra-files/config-files/mediastorage.conf";
//
//        final Properties prop = new Properties();
//        final InputStream input = new FileInputStream(configFilePath);
//        prop.load(input);
//
//        final String host = prop.getProperty("host");
//        final Integer port = Integer.valueOf(prop.getProperty("port"));
//        final String dbName = prop.getProperty("dbName");
//        final String namespaceName = prop.getProperty("nameSpace");
//        final String username = prop.getProperty("username");
//        final String password = prop.getProperty("password");
//
//        final MediaStorageClientConfig config =
//                new MediaStorageClientConfig(host, port, username, password, dbName, namespaceName);
//
//        client = new MediaStorageClientImpl(config);
//    }
//
//    @org.junit.Test
//    public void testCheckIfExists() throws Exception {
//        Boolean exist = client.checkIfExists(getMD5("testUrl"));
//        assertFalse(exist);
//
//        String id = null;
//        final Path path = Paths.get("./extra-files/config-files/mediastorage.conf");
//        try {
//            final byte[] data = Files.readAllBytes(path);
//            final MediaFile mediaFile = new MediaFile("testSource", "testName", new ArrayList<String>(), "",
//                    "testUrl", new DateTime(), data, 1, "testContentType", new HashMap<String, String>(), 200);
//
//            client.createOrModify(mediaFile);
//
//            id = mediaFile.getId();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        exist = client.checkIfExists(id);
//        assertTrue(exist);
//
//        client.delete(id);
//    }
//
//    @org.junit.Test
//    public void testRetrieveWithContent() throws Exception {
//        Boolean exist = client.checkIfExists(getMD5("testUrl"));
//        assertFalse(exist);
//
//        final Path path = Paths.get("./extra-files/config-files/mediastorage.conf");
//        try {
//            final byte[] data = Files.readAllBytes(path);
//            final MediaFile mediaFile = new MediaFile("testSource", "testName", new ArrayList<String>(),
//                    "", "testUrl", new DateTime(), data, 1, "testContentType", new HashMap<String, String>(), 200);
//
//            client.createOrModify(mediaFile);
//
//            final String id = mediaFile.getId();
//
//            final MediaFile retrievedMediaFile = client.retrieve(id, true);
//            assertNotNull(retrievedMediaFile);
//            assertArrayEquals(retrievedMediaFile.getContent(), data);
//
//            client.delete(id);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @org.junit.Test
//    public void testRetrieveWithoutContent() throws Exception {
//        Boolean exist = client.checkIfExists(getMD5("testUrl"));
//        assertFalse(exist);
//
//        final Path path = Paths.get("./extra-files/config-files/mediastorage.conf");
//        try {
//            final byte[] data = Files.readAllBytes(path);
//            final MediaFile mediaFile = new MediaFile("testSource", "testName", new ArrayList<String>(),
//                    "", "testUrl", new DateTime(), data, 1, "testContentType", new HashMap<String, String>(), 200);
//
//            client.createOrModify(mediaFile);
//
//            final String id = mediaFile.getId();
//
//            final MediaFile retrievedMediaFile = client.retrieve(id, false);
//            assertNotNull(retrievedMediaFile);
//            assertNull(retrievedMediaFile.getContent());
//
//            client.delete(id);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @org.junit.Test
//    public void testCreateOrModify() throws Exception {
//        Boolean exist = client.checkIfExists(getMD5("testUrl"));
//        assertFalse(exist);
//
//        final Path path = Paths.get("./extra-files/config-files/mediastorage.conf");
//        final Path path2 = Paths.get("./extra-files/config-files/slave.conf");
//        try {
//            final byte[] data = Files.readAllBytes(path);
//            final byte[] data2 = Files.readAllBytes(path2);
//            final MediaFile mediaFile = new MediaFile("testSource", "testName", new ArrayList<String>(),
//                    "", "testUrl", new DateTime(), data, 1, "testContentType", new HashMap<String, String>(), 200);
//
//            client.createOrModify(mediaFile);
//
//            final String id = mediaFile.getId();
//
//            final MediaFile retrievedMediaFile = client.retrieve(id, true);
//            assertNotNull(retrievedMediaFile);
//            assertArrayEquals(retrievedMediaFile.getContent(), data);
//
//            final MediaFile updatedMediaFile = new MediaFile("testSource", "testName", new ArrayList<String>(),
//                    "", "testUrl", new DateTime(), data2, 1, "testContentType", new HashMap<String, String>(), 200);
//            client.createOrModify(updatedMediaFile);
//
//            final MediaFile retrievedUpdatedMediaFile = client.retrieve(id, true);
//            assertNotNull(retrievedUpdatedMediaFile);
//            assertArrayEquals(retrievedUpdatedMediaFile.getContent(), data2);
//
//            client.delete(id);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @org.junit.Test
//    public void testDelete() throws Exception {
//        Boolean exist = client.checkIfExists(getMD5("testUrl"));
//        assertFalse(exist);
//
//        String id = null;
//        final Path path = Paths.get("./extra-files/config-files/mediastorage.conf");
//        try {
//            final byte[] data = Files.readAllBytes(path);
//            final MediaFile mediaFile = new MediaFile("testSource", "testName", new ArrayList<String>(), "",
//                    "testUrl", new DateTime(), data, 1, "testContentType", new HashMap<String, String>(), 200);
//
//            client.createOrModify(mediaFile);
//
//            id = mediaFile.getId();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        exist = client.checkIfExists(id);
//        assertTrue(exist);
//
//        client.delete(id);
//
//        exist = client.checkIfExists(id);
//        assertFalse(exist);
//    }
//
//    private String getMD5(String input) {
//        final MessageDigest messageDigest;
//        String temp;
//        try {
//            messageDigest = MessageDigest.getInstance("MD5");
//            messageDigest.reset();
//            messageDigest.update(input.getBytes());
//            final byte[] resultByte = messageDigest.digest();
//            StringBuffer sb = new StringBuffer();
//            for (byte aResultByte : resultByte) {
//                sb.append(Integer.toString((aResultByte & 0xff) + 0x100, 16).substring(1));
//            }
//            temp = sb.toString();
//        } catch (NoSuchAlgorithmException e) {
//            temp = input;
//        }
//
//        return temp;
//    }
//
//    @Test
//    public void testImage() throws IOException, NoSuchAlgorithmException {
//        // Look for a valid id
//        final MediaFile mediaFile = client.retrieve("3461671ffc4acf7d501f05d60542999b", true);
//        OutputStream out = new BufferedOutputStream(new FileOutputStream("test"));
//        out.write(mediaFile.getContent());
//        out.close();
//    }

    @Test
    public void test(){

    }
}