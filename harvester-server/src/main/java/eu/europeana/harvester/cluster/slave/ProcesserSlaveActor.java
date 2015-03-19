package eu.europeana.harvester.cluster.slave;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.ResponseType;
import eu.europeana.harvester.utils.FileUtils;
import eu.europeana.harvester.utils.MediaMetaDataUtils;
import eu.europeana.harvester.utils.ThumbnailUtils;
import org.im4java.core.IM4JavaException;
import org.im4java.core.InfoException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This type of actor extracts the metadata from a downloaded document,
 * and if it is an image creates a thumbnail from it.
 */
public class ProcesserSlaveActor extends UntypedActor {

    private final static File tmpFolder = new File("/tmp/europeana");

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

    /**
     * The module which stores the thumbnails in MongoDB.
     */
    private final MediaStorageClient mediaStorageClient;

    private final String source;

    /**
     * Path to a file needed for the colormap extraction.
     */
    private final String colorMapPath;

    /**
     * A list of created thumbnails.
     */
    private final List<MediaFile> thumbnails = new ArrayList<>();

    public ProcesserSlaveActor(final ResponseType responseType, MediaStorageClient mediaStorageClient, String source, String colorMapPath) {
        this.responseType = responseType;
        this.mediaStorageClient = mediaStorageClient;
        this.source = source;
        this.colorMapPath = colorMapPath;

        LOG.info("ProcesserSlaveActor constructor");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof DoneDownload) {
            doneDownload = (DoneDownload) message;

            path = doneDownload.getHttpRetrieveResponse().getAbsolutePath();
            if (path.equals("")) {
                path = FileUtils.createFileAndFolderIfMissing(LOG, tmpFolder, doneDownload.getReferenceId(), doneDownload.getHttpRetrieveResponse().getContent());
            }
            error = "";

            final ProcessingJobTaskDocumentReference task = doneDownload.getDocumentReferenceTask();
            startProcessing(task);

            return;
        }
    }

    /**
     * The brain of the actor. Here starts everything in the processing part.
     *
     * @param task an object which contains the task with the associated subtasks.
     * @throws InterruptedException
     * @throws IOException
     */
    private void startProcessing(final ProcessingJobTaskDocumentReference task) throws Exception {
        final List<ProcessingJobSubTask> subTasks = task.getProcessingTasks();

        DoneProcessing doneProcessing = null;
        Boolean isThumbnail = false;
        ImageMetaInfo metaInfoForThumbnails = null;

        for (ProcessingJobSubTask subTask : subTasks) {
            switch (subTask.getTaskType()) {
                case META_EXTRACTION:
                    doneProcessing = metadataExtraction();

                    break;
                case GENERATE_THUMBNAIL:
                    final GenericSubTaskConfiguration config = subTask.getConfig();
                    generateThumbnail(config);

                    break;
                case COLOR_EXTRACTION:
                    metaInfoForThumbnails = MediaMetaDataUtils.colorExtraction(path, colorMapPath);
                    isThumbnail = true;
                    if (metaInfoForThumbnails == null) {
                        doneProcessing = new DoneProcessing(doneDownload, null, null, null, null);
                        break;
                    }

                    if (doneProcessing != null) {
                        doneProcessing = doneProcessing.withColorPalette(metaInfoForThumbnails);
                    } else {
                        doneProcessing = new DoneProcessing(doneDownload, metaInfoForThumbnails, null, null, null);
                    }

                    break;
                default:
                    LOG.info("Unknown subtask in job: {}, referenceId: {}", doneDownload.getJobId(),
                            doneDownload.getReferenceId());
            }
        }

        if (isThumbnail) {
            for (MediaFile thumbnail : thumbnails) {
                if (metaInfoForThumbnails != null) {
                    final MediaFile newThumbnail = thumbnail.withMetaInfo(metaInfoForThumbnails.getColorPalette());
                    mediaStorageClient.createOrModify(newThumbnail);
                } else {
                    mediaStorageClient.createOrModify(thumbnail);
                }
            }
            thumbnails.clear();
        }

        if (error.length() != 0 && doneProcessing != null) {
            doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR, error);
        }
        if (doneProcessing == null) {
            doneProcessing = new DoneProcessing(doneDownload, null, null, null, null);
            doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR, "Error in processing");
        }

        deleteFile();
        getSender().tell(doneProcessing, getSelf());
    }

    /**
     * Deletes a file.
     */
    private void deleteFile() {
        if (!doneDownload.getHttpRetrieveResponse().getAbsolutePath().equals("")) {
            return;
        }

        final File file = new File(path);
        file.delete();
    }

    /**
     * Extracts all the metadata from a file.
     *
     * @return an object with metadata and additional information about the processing.
     * @throws InterruptedException
     */
    private DoneProcessing metadataExtraction() throws InterruptedException {
        DoneProcessing doneProcessing = null;
        try {
            doneProcessing = extract();
        } catch (Exception e) {
            error = "In ProcesserSlaveActor: " + e.getMessage();
        }

        if (doneProcessing != null) {
            doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR, error);
        }
        return doneProcessing;
    }

    /**
     * Generates thumbnail from an image.
     *
     * @param config
     * @throws InterruptedException
     */
    private void generateThumbnail(GenericSubTaskConfiguration config) throws Exception {
        if (MediaMetaDataUtils.classifyUrl(path).equals(ContentType.IMAGE)) {
            thumbnails.add(ThumbnailUtils.createMediaFileWithThumbnail(config.getThumbnailConfig(), source, doneDownload.getUrl(), doneDownload.getHttpRetrieveResponse().getContent(), path, colorMapPath));
        }
    }


    /**
     * Classifies the document and performs the specific operations for this actor.
     *
     * @return - the response message.
     */
    private DoneProcessing extract() {
        final ContentType contentType = MediaMetaDataUtils.classifyUrl(path);

        ImageMetaInfo imageMetaInfo = null;
        AudioMetaInfo audioMetaInfo = null;
        VideoMetaInfo videoMetaInfo = null;
        TextMetaInfo textMetaInfo = null;
        try {
            if (!responseType.equals(ResponseType.NO_STORAGE)) {
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
            }
        } catch (InfoException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        } catch (InterruptedException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        } catch (IM4JavaException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        } catch (IOException e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        } catch (Exception e) {
            error = "In ProcesserSlaveActor: \n" + e.toString();
        }

        return new DoneProcessing(doneDownload, imageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo);
    }

}
