package eu.europeana.harvester.cluster.slave.processing;

import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaDataUtils;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoTuple;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.ResponseType;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SlaveProcessor {

    private final MediaMetaInfoExtractor metaInfoExtractor;
    private final ThumbnailGenerator thumbnailGenerator;
    private final ColorExtractor colorExtractor;
    private final MediaStorageClient mediaStorageClient;
    private final LoggingAdapter LOG;

    public SlaveProcessor(MediaMetaInfoExtractor metaInfoExtractor, ThumbnailGenerator thumbnailGenerator, ColorExtractor colorExtractor, MediaStorageClient mediaStorageClient, LoggingAdapter LOG) {
        this.metaInfoExtractor = metaInfoExtractor;
        this.thumbnailGenerator = thumbnailGenerator;
        this.colorExtractor = colorExtractor;
        this.mediaStorageClient = mediaStorageClient;
        this.LOG = LOG;
    }

    public ProcessingResultTuple process(final ProcessingJobTaskDocumentReference task, String originalFilePath, String originalFileUrl, byte[] originalFileContent,
                                         ResponseType responseType, String processingProcessId) throws Exception {

        ProcessingJobSubTask metaExtractionProcessingTask = null;
        ProcessingJobSubTask colorExtractionProcessingTask = null;
        List<ProcessingJobSubTask> thumbnailGenerationProcessingTasks = new ArrayList();

        MediaMetaInfoTuple mediaMetaInfoTuple = null;
        final Map<ProcessingJobSubTask, MediaFile> generatedThumbnails = new HashMap<ProcessingJobSubTask, MediaFile>();
        ImageMetaInfo imageColorMetaInfo = null;

        // Step 1 : Pickup the sub tasks from the configuration.
        for (final ProcessingJobSubTask subTask : task.getProcessingTasks()) {
            switch (subTask.getTaskType()) {
                case META_EXTRACTION:
                    if (metaExtractionProcessingTask == null) {
                        metaExtractionProcessingTask = subTask;
                    } else {
                        throw new IllegalArgumentException("Cannot process configuration for url : " + originalFileUrl + ". The configuration contains more than one meta extraction subtask. Should be exactly one.");
                    }
                    break;
                case COLOR_EXTRACTION:
                    if (colorExtractionProcessingTask == null) {
                        colorExtractionProcessingTask = subTask;
                    } else {
                        throw new IllegalArgumentException("Cannot process configuration for url : " + originalFileUrl + ". The configuration contains more than one colour extraction subtask. Should be exactly one.");
                    }
                    break;
                case GENERATE_THUMBNAIL:
                    thumbnailGenerationProcessingTasks.add(subTask);
                    break;
                default:
                    LOG.error("Configuration for url " + originalFileUrl + " has an unknown sub task type " + subTask);
                    break;
            }
        }


        // Step 2 : Execute the tasks

        // Extract meta info
        if (metaExtractionProcessingTask != null) {
            if (responseType.equals(ResponseType.NO_STORAGE)) {
                throw new IllegalArgumentException("Configuration for url " + originalFileUrl + " for sub task " + metaExtractionProcessingTask.getConfig() + "Cannot execute meta info extraction because the media file is not stored.");
            }
            mediaMetaInfoTuple = metaInfoExtractor.extract(originalFilePath);
        }

        // Generate thumbnails
        for (ProcessingJobSubTask thumbnailGenerationTask : thumbnailGenerationProcessingTasks) {
            if (MediaMetaDataUtils.classifyUrl(originalFilePath).equals(ContentType.IMAGE)) {
                final GenericSubTaskConfiguration config = thumbnailGenerationTask.getConfig();
                generatedThumbnails.put(thumbnailGenerationTask, thumbnailGenerator.createMediaFileWithThumbnail(config.getThumbnailConfig().getHeight(),
                        config.getThumbnailConfig().getWidth(), processingProcessId, originalFileUrl,
                        originalFileContent, originalFilePath));
            }
        }

        // Extra color palette
        if (colorExtractionProcessingTask != null) {
            if (MediaMetaDataUtils.classifyUrl(originalFilePath).equals(ContentType.IMAGE)) {
                imageColorMetaInfo = colorExtractor.colorExtraction(originalFilePath);

                // Insert the color pallete in all the thumbnail meta infos
                for (final Map.Entry<ProcessingJobSubTask, MediaFile> thumbnailEntry : generatedThumbnails.entrySet()) {
                    generatedThumbnails.put(thumbnailEntry.getKey(), thumbnailEntry.getValue().withColorPalette(imageColorMetaInfo.getColorPalette()));
                }
            }
        }

        // Step 3 : Execute the end of processing operations

        for (final Map.Entry<ProcessingJobSubTask, MediaFile> thumbnailEntry : generatedThumbnails.entrySet()) {
            mediaStorageClient.createOrModify(thumbnailEntry.getValue());
            deleteFile(originalFilePath);
        }

        LOG.info("Done processing for url ", originalFileUrl);
        return new ProcessingResultTuple(mediaMetaInfoTuple, generatedThumbnails.values(), imageColorMetaInfo);
    }

    /**
     * Deletes a file.
     */
    private void deleteFile(String path) {
        final File file = new File(path);
        file.delete();
    }

}
