package eu.europeana.harvester.cluster.slave;

import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.ResponseType;
import eu.europeana.harvester.utils.MediaMetaDataUtils;
import eu.europeana.harvester.utils.ThumbnailUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nutz on 21.05.2015.
 */
public class ProcessorHelperProcess {

    private final List<MediaFile> thumbnails = new ArrayList<>();
    private LoggingAdapter LOG = null;
    private String error;


    public ProcessorHelperProcess ( LoggingAdapter LOG ) {
        this.LOG = LOG;
    }





    public DoneProcessing startProcessing(final ProcessingJobTaskDocumentReference task, DoneDownload doneDownload, String path, String colorMapPath,
                                           MediaStorageClient mediaStorageClient, ResponseType responseType, String source) throws Exception {
        final List<ProcessingJobSubTask> subTasks = task.getProcessingTasks();

        DoneProcessing doneProcessing = null;
        Boolean isThumbnail = false;
        ImageMetaInfo metaInfoForThumbnails = null;



        try {

            for (ProcessingJobSubTask subTask : subTasks) {
                switch (subTask.getTaskType()) {
                    case META_EXTRACTION:
                        doneProcessing = extract(doneDownload, path, colorMapPath, responseType);

                        break;
                    case GENERATE_THUMBNAIL:
                        final GenericSubTaskConfiguration config = subTask.getConfig();
                        generateThumbnail(config, doneDownload, path, source, colorMapPath);

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
            deleteFile(doneDownload, path);
        } catch (Exception e) {
            LOG.info("Error in startProcessing : {}",e.getMessage());
        }

        if (error.length() != 0 && doneProcessing != null) {
            doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR, error);
        }
        if (doneProcessing == null) {
            doneProcessing = new DoneProcessing(doneDownload, null, null, null, null);
            doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR, "Error in processing");
        }

        //deleteFile();
        LOG.info("Done processing for task ID {}, telling the master ", doneProcessing.getTaskID());
        return doneProcessing;
//        getSender().tell(doneProcessing, getSelf());
//        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    /**
     * Deletes a file.
     */
    private void deleteFile(DoneDownload doneDownload, String path) {
        if (!doneDownload.getHttpRetrieveResponse().getAbsolutePath().equals("")) {
            return;
        }

        final File file = new File(path);
        file.delete();
    }


    /**
     * Generates thumbnail from an image.
     *
     * @param config
     * @throws InterruptedException
     */
    private void generateThumbnail(GenericSubTaskConfiguration config, DoneDownload doneDownload, String path, String source, String colorMapPath) throws Exception {
        if (MediaMetaDataUtils.classifyUrl(path).equals(ContentType.IMAGE)) {
            thumbnails.add(ThumbnailUtils.createMediaFileWithThumbnail(config.getThumbnailConfig(), source, doneDownload.getUrl(), doneDownload.getHttpRetrieveResponse().getContent(), path, colorMapPath));
        }
    }


    /**
     * Classifies the document and performs the specific operations for this actor.
     *
     * @return - the response message.
     */
    private DoneProcessing extract(DoneDownload doneDownload, String path, String colorMapPath, ResponseType responseType ) {
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
            return new DoneProcessing(doneDownload, imageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo);
        } catch (Exception e) {
            LOG.info("Error in Processing slave - extract : {}",e.getMessage());
            return new DoneProcessing(doneDownload, imageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo).withNewState(ProcessingState.ERROR,error);
        }

    }

}
