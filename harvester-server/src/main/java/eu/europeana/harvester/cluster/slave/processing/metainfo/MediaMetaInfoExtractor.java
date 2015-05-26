package eu.europeana.harvester.cluster.slave.processing.metainfo;

import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.domain.AudioMetaInfo;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.TextMetaInfo;
import eu.europeana.harvester.domain.VideoMetaInfo;

public class MediaMetaInfoExtractor {
    private final String colorMapPath;

    public MediaMetaInfoExtractor(String colorMapPath) {
        this.colorMapPath = colorMapPath;
    }

    public final MediaMetaInfoTuple extract(final String path) throws Exception {
        final ContentType contentType = MediaMetaDataUtils.classifyUrl(path);

        ImageMetaInfo imageMetaInfo = null;
        AudioMetaInfo audioMetaInfo = null;
        VideoMetaInfo videoMetaInfo = null;
        TextMetaInfo textMetaInfo = null;
        switch (contentType) {
            case TEXT:
                textMetaInfo = MediaMetaDataUtils.extractTextMetaData(path);
                break;
            case IMAGE:
                imageMetaInfo = MediaMetaDataUtils.extractImageMetadata(path, colorMapPath);
                break;
            case VIDEO:
                videoMetaInfo = MediaMetaDataUtils.extractVideoMetaData(path);
                break;
            case AUDIO:
                audioMetaInfo = MediaMetaDataUtils.extractAudioMetadata(path);
                break;
            case UNKNOWN:
                break;
        }
        return new MediaMetaInfoTuple(imageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo);

    }


}
