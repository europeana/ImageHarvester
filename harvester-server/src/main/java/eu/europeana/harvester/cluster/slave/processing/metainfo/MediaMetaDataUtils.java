package eu.europeana.harvester.cluster.slave.processing.metainfo;

import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.utils.LocalMediaChecker;
import gr.ntua.image.mediachecker.AudioInfo;
import gr.ntua.image.mediachecker.ImageInfo;
import gr.ntua.image.mediachecker.MediaChecker;
import gr.ntua.image.mediachecker.VideoInfo;
import org.im4java.core.IM4JavaException;

import java.io.IOException;

public class MediaMetaDataUtils {

    /**
     * Classifies the downloaded content in one of the existent categories.
     *
     * @return - the matching category
     */
    public static final ContentType classifyUrl(final String path) {
        try {
            final String type = MediaChecker.getMimeType(path);

            if (type.startsWith("image")) {
                return ContentType.IMAGE;
            }
            if (type.startsWith("audio")) {
                return ContentType.AUDIO;
            }
            if (type.startsWith("video")) {
                return ContentType.VIDEO;
            }
            if (type.equals("application/pdf")) {
                return ContentType.PDF;
            }
            if ((type.startsWith("text") || type.equals("application/xml") || type.equals("application/rtf") || type.equals("application/epub"))
                    && !type.contains("html")) {
                return ContentType.NON_PDF_TEXT;
            }
            return ContentType.UNKNOWN;
        } catch (IOException e) {
            return ContentType.UNKNOWN;
            // IT's OK to eat the exception here.
        }
    }



    /**
     * Extracts image meta data
     * @return - an object with all the meta info
     */
    public final static ImageMetaInfo extractImageMetadata(final String path, final String colorMapPath) throws InterruptedException, IOException, IM4JavaException {
        ImageMetaInfo imageMetaInfo = null;

            final ImageInfo imageInfo = MediaChecker.getImageInfo(path, colorMapPath);
            final Long fileSize = MediaChecker.getFileSize(path);

            ImageOrientation imageOrientation;
            if(imageInfo.getWidth() > imageInfo.getHeight()) {
                imageOrientation = ImageOrientation.LANDSCAPE;
            } else {
                imageOrientation = ImageOrientation.PORTRAIT;
            }

            imageMetaInfo = new ImageMetaInfo(imageInfo.getWidth(), imageInfo.getHeight(),
                    imageInfo.getMimeType(), imageInfo.getFileFormat(), imageInfo.getColorSpace(),
                    fileSize, imageInfo.getPalette(), imageOrientation);

        return imageMetaInfo;
    }

    /**
     * Extracts audio meta data
     * @return - an object with all the meta info
     */
    public final static AudioMetaInfo extractAudioMetadata(final String path) throws IOException {
        AudioMetaInfo audioMetaInfo = null;

            final AudioInfo audioInfo = MediaChecker.getAudioInfo(path);
            final Long fileSize = MediaChecker.getFileSize(path);

            audioMetaInfo = new AudioMetaInfo(audioInfo.getSampleRate(), audioInfo.getBitRate(),
                    audioInfo.getDuration(), audioInfo.getMimeType(), audioInfo.getFileFormat(), fileSize,
                    audioInfo.getChannels(), audioInfo.getBitDepth());


        return audioMetaInfo;
    }

    /**
     * Extracts video meta data
     * @return - an object with all the meta info
     */
    public final static VideoMetaInfo extractVideoMetaData(final String path) throws IOException {
        VideoMetaInfo videoMetaInfo = null;

            final VideoInfo videoInfo = MediaChecker.getVideoInfo(path);
            final Long fileSize = MediaChecker.getFileSize(path);

            videoMetaInfo = new VideoMetaInfo(videoInfo.getWidth(), videoInfo.getHeight(), videoInfo.getDuration(),
                    videoInfo.getMimeType(), videoInfo.getFrameRate(), fileSize,
                    videoInfo.getCodec(), videoInfo.getResolution(), videoInfo.getBitRate());

        return videoMetaInfo;
    }

    /**
     * Extracts text meta data
     * @return - an object with all the meta info
     */
    public final static TextMetaInfo extractTextMetaData(final String path) throws Exception {
        TextMetaInfo textMetaInfo ;

            final Long fileSize = MediaChecker.getFileSize(path);
            final Boolean isSearchable = LocalMediaChecker.issearchable(path);

            Integer getDPI = null;
            try {
                getDPI = LocalMediaChecker.getdpi(path);
            } catch(Exception e) {}

            textMetaInfo = new TextMetaInfo(MediaChecker.getMimeType(path), fileSize,
                    getDPI, isSearchable);

        return textMetaInfo;
    }


}
