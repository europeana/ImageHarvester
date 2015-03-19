package test.utils;

import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.ThumbnailConfig;
import eu.europeana.harvester.utils.ThumbnailUtils;

import java.nio.file.FileSystems;
import java.nio.file.Files;

public class ThumbnailsUtilsTest {
    public static void main(String[] args) throws Exception {
        final byte[] fileContent = Files.readAllBytes(FileSystems.getDefault().getPath("/Users/paul/Documents/workspace/europeana/harvester-server/src/test/resources/", "big_image.jpg"));
        MediaFile res = ThumbnailUtils.createMediaFileWithThumbnail(new ThumbnailConfig(180, 180), "source1", "url1", fileContent, "/Users/paul/Documents/workspace/europeana/harvester-server/src/test/resources/big_image.jpg", "/Users/paul/Documents/workspace/europeana/colormap.png");
        Files.write(FileSystems.getDefault().getPath("/Users/paul/Documents/workspace/europeana/harvester-server/src/test/resources/", "big_image_res.jpg"), res.getContent());
    }
}
