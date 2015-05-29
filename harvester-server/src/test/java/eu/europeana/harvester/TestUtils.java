package eu.europeana.harvester;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by salexandru on 29.05.2015.
 */
public class TestUtils {
    public static final String PATH_PREFIX = Paths.get("/harvester-server/src/test/resources/").toAbsolutePath().toString() + "/";
    public static String PATH_COLORMAP = PATH_PREFIX + "colormap.png";
    public static final String PATH_DOWNLOADED = PATH_PREFIX + "downloader/";
    public static final String GitHubUrl_PREFIX = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/";


    public static final String Image1 = "image1.jpeg";
    public static final String Image1ThumbnailSmall = "image1_thumbnail_small.jpeg";
    public static final String Image1ThumbnailMedium = "image1_thumbnail_medium.jpeg";
    public static final String Image1ThumbnailLarge = "image1_thumbnail_large.jpeg";

    public static final Map<String, byte[]> imagesInBytes = new HashMap<>();

    public static final String Image2 = "image2.jpeg";
    public static final String Image2ThumbnailSmall = "image2_thumbnail_small.jpeg";
    public static final String Image2ThumbnailMedium = "image2_thumbnail_medium.jpeg";
    public static final String Image2ThumbnailLarge = "image2_thumbnail_large.jpeg";

    public static final String Audio1 = "audio1.mp3";
    public static final String Audio2 = "audio2.mp3";
    public static final String Video1 = "video1.mpg";
    public static final String Video2 = "video2.mpg";
    public static final String Text1 = "text1.pdf";
    public static final String Text2 = "text2.pdf";

    public static final String IMAGE_MIMETYPE = "image/jpeg";
    public static final String AUDIO_MIMETYPE = "audio/mpeg";
    public static final String VIDEO_MIMETYPE = "video/mpeg";
    public static final String TEXT_MIMETYPE  = "application/octet-stream";

    public static final String IMAGE_FORMAT = "JPEG";
    public static final String AUDIO_FORMAT = "MP3";
    public static final String VIDEO_FORMAT = "mpeg1video";


    public static String getPath(String fileName) {return PATH_PREFIX + fileName;}

    static {
        try {
            imagesInBytes.put(Image1, Files.toByteArray(new File(getPath(Image1))));
            imagesInBytes.put(Image1ThumbnailSmall, Files.toByteArray(new File(getPath(Image1ThumbnailSmall))));
            imagesInBytes.put(Image1ThumbnailMedium, Files.toByteArray(new File(getPath(Image1ThumbnailMedium))));
            imagesInBytes.put(Image1ThumbnailLarge, Files.toByteArray(new File(getPath(Image1ThumbnailLarge))));
            imagesInBytes.put(Image2, Files.toByteArray(new File(getPath(Image2))));
            imagesInBytes.put(Image2ThumbnailSmall, Files.toByteArray(new File(getPath(Image2ThumbnailSmall))));
            imagesInBytes.put(Image2ThumbnailMedium, Files.toByteArray(new File(getPath(Image2ThumbnailMedium))));
            imagesInBytes.put(Image2ThumbnailLarge, Files.toByteArray(new File(getPath(Image2ThumbnailLarge))));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
