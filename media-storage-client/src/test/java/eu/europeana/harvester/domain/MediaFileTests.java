package eu.europeana.harvester.domain;

import org.junit.Test;

import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertEquals;

public class MediaFileTests {

    public static final String gitHubUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/";

    @Test
    public void canGenerateCorrectIds() throws NoSuchAlgorithmException {
        final String urlHash = "f4793f5287925d1cd7c51faab8a64401";
        final String id = MediaFile.generateIdFromUrlAndSizeType(gitHubUrl,"ORIGINAL");
        assertEquals(urlHash+"-"+"ORIGINAL",id);
    }
}
