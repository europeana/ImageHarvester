package eu.europeana.harvester.cluster.slave.processing.color;

import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaDataUtils;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.ProcessingJobSubTaskState;
import gr.ntua.image.mediachecker.MediaChecker;

import java.io.IOException;

public class ColorExtractor {
    private final String colorMapPath;

    public ColorExtractor(String colorMapPath) {
        this.colorMapPath = colorMapPath;
    }

    /**
     * Extracts the colormap from an image.
     *
     * @return partial metainfo, contains only the colormap
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    public final ImageMetaInfo colorExtraction(final String path) throws IOException, InterruptedException {
        boolean success = true;
        int retry = 3;
        do {
            try {
                if (MediaChecker.getMimeType(path).startsWith("image")) {
                    final ImageMetaInfo imageMetaInfo = MediaMetaDataUtils.extractImageMetadata(path, colorMapPath);
                    return new ImageMetaInfo(null, null, null, null, null, null, imageMetaInfo.getColorPalette(), null);
                }
            } catch (Exception e) {
                if (retry > 0) {
                    success = false;
                    Thread.sleep(1000);
                    retry = -1;
                } else {
                    success = true;
                }
            }
        } while (!success);

        return null;
    }

}
