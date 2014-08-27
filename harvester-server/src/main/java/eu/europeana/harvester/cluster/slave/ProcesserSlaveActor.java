package eu.europeana.harvester.cluster.slave;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.corelib.utils.ImageUtils;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.domain.ThumbnailConfig;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.domain.AudioMetaInfo;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.domain.VideoMetaInfo;
import eu.europeana.harvester.httpclient.response.ResponseType;
import gr.ntua.image.mediachecker.AudioInfo;
import gr.ntua.image.mediachecker.ImageInfo;
import gr.ntua.image.mediachecker.MediaChecker;
import gr.ntua.image.mediachecker.VideoInfo;
import org.im4java.core.IM4JavaException;
import org.im4java.core.InfoException;

import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * This type of actor extracts the metadata from a downloaded document,
 * and if it is an image creates a thumbnail from it.
 */
public class ProcesserSlaveActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * If the response type is disk storage then the absolute path on disk where
     * the content of the download will be saved.
     */
    private String path;

    /**
     * Response type: memory or disk storage.
     */
    private final ResponseType responseType;

    /**
     * The response from the downloader actor.
     */
    private DoneDownload doneDownload;

    /**
     * The error message if any error occurs.
     */
    private String error;

    public ProcesserSlaveActor(final ResponseType responseType) {
        this.responseType = responseType;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof DoneDownload) {
            doneDownload = (DoneDownload) message;

            path = doneDownload.getHttpRetrieveResponse().getAbsolutePath();
            error = "";

            boolean success = true;
            int retry = 10;
            DoneProcessing doneProcessing = null;
            do {
                try {
                    doneProcessing = extract();
                } catch(Exception e) {
                    if(retry > 0) {
                        success = false;
                        Thread.sleep(1000);
                        retry =- 1;
                    } else {
                        doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR ,
                                "In ProcesserSlaveActor: \n" + e.toString());
                    }
                }
            } while(!success);

            if(error.length() != 0) {
                doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR, error);
            }

            getSender().tell(doneProcessing, getSelf());

            return;
        }
    }

    /**
     * Classifies the document and performes the specific operations for this actor.
     * @return - the response message.
     */
    private DoneProcessing extract() {
        final ContentType contentType = classifyUrl();

        ImageMetaInfo imageMetaInfo = null;
        AudioMetaInfo audioMetaInfo = null;
        VideoMetaInfo videoMetaInfo = null;

        if(!responseType.equals(ResponseType.NO_STORAGE)) {
            switch (contentType) {
                case TEXT:
                    break;
                case IMAGE:
                    imageMetaInfo = extractImageMetadata();
                    //TODO: uncomment
                    //createThumbnail();
                    break;
                case VIDEO:
                    videoMetaInfo = extractVideoMetaData();
                    break;
                case AUDIO:
                    audioMetaInfo = extractAudioMetadata();
                    break;
                case UNKNOWN:
                    break;
            }
        }

        return new DoneProcessing(doneDownload, imageMetaInfo, audioMetaInfo, videoMetaInfo);
    }

    /**
     * Classifies the downloaded content in one of the existent categories.
     * @return - the matching category
     */
    private ContentType classifyUrl() {
        try {
            String type = MediaChecker.getMimeType(path);
            if(type.startsWith("image")) {
                return ContentType.IMAGE;
            }
            if(type.startsWith("audio")) {
                return ContentType.AUDIO;
            }
            if(type.startsWith("video")) {
                return ContentType.VIDEO;
            }
            if(type.startsWith("text")) {
                return ContentType.TEXT;
            }

        } catch (IOException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        }

        return ContentType.UNKNOWN;
    }

    /**
     * Extracts image meta data
     * @return - an object with all the meta info
     */
    private ImageMetaInfo extractImageMetadata() {
        ImageMetaInfo imageMetaInfo = null;
        final String colorMapPath = "./harvester-server/src/main/resources/colormap.png";

        try {
            final ImageInfo imageInfo = MediaChecker.getImageInfo(path, colorMapPath);

            imageMetaInfo =
                    new ImageMetaInfo(imageInfo.getWidth(), imageInfo.getHeight(), imageInfo.getMimeType(),
                            imageInfo.getFileFormat(), imageInfo.getColorSpace());

        } catch (IOException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        } catch (InfoException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        } catch (InterruptedException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        } catch (IM4JavaException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        }

        return imageMetaInfo;
    }

    /**
     * Extracts audio meta data
     * @return - an object with all the meta info
     */
    private AudioMetaInfo extractAudioMetadata() {
        AudioMetaInfo audioMetaInfo = null;

        try {
            final AudioInfo audioInfo = MediaChecker.getAudioInfo(path);

            audioMetaInfo =
                    new AudioMetaInfo(audioInfo.getSampleRate(), audioInfo.getBitRate(), audioInfo.getDuration(),
                            audioInfo.getMimeType(), audioInfo.getFileFormat());
        } catch (IOException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        }

        return audioMetaInfo;
    }

    /**
     * Extracts video meta data
     * @return - an object with all the meta info
     */
    private VideoMetaInfo extractVideoMetaData() {
        VideoMetaInfo videoMetaInfo = null;

        try {
            final VideoInfo videoInfo = MediaChecker.getVideoInfo(path);

            videoMetaInfo =
                    new VideoMetaInfo(videoInfo.getWidth(), videoInfo.getHeight(), videoInfo.getDuration(),
                            videoInfo.getMimeType(), videoInfo.getFileFormat(), videoInfo.getFrameRate());
        } catch (IOException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        }

        return videoMetaInfo;
    }

    /**
     * Creates a thumbnail of a downloaded image.
     */
    private void createThumbnail() {
        final ThumbnailConfig thumbnailConfig = doneDownload.getJobConfigs().getThumbnailConfig();
        try {
            final int width = thumbnailConfig.getWidth();
            final int height = thumbnailConfig.getHeight();

            final byte[] data = doneDownload.getHttpRetrieveResponse().getContent();
            final BufferedImage oldImage = ImageUtils.toBufferedImage(data);
            final BufferedImage newImage = ImageUtils.scale(oldImage, width, height);
            final byte[] newData = ImageUtils.toByteArray(newImage);

            doneDownload.getHttpRetrieveResponse().init();
            doneDownload.getHttpRetrieveResponse().addContent(newData);
            doneDownload.getHttpRetrieveResponse().close();
        } catch (IOException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        } catch (Exception e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        }
    }
}
