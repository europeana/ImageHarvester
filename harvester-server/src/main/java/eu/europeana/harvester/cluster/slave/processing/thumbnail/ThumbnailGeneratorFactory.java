package eu.europeana.harvester.cluster.slave.processing.thumbnail;

import eu.europeana.harvester.cluster.domain.ContentType;

/**
 * Created by andra on 14.06.2016.
 * Factory class to switch between {@link ThumbnailGenerator} object types
 */

public class ThumbnailGeneratorFactory {
    /**
     * @param contentType the MIME type of the file you are generating thumbnail for
     * @param colorMapPath set in config
     * @return a new ThumbnailGenerator instance
     * @throws Exception
     */
    public static ThumbnailGenerator getThumbnailGenerator(final ContentType contentType, final String colorMapPath) throws Exception {
        switch(contentType) {
            case IMAGE: return new ThumbnailImageGenerator(colorMapPath);

            case PDF: return new ThumbnailTextGenerator(colorMapPath);

            default :  throw new Exception("No thumbnail supported for " + contentType + " content type.");
        }
    }
}
