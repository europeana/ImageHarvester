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
import eu.europeana.harvester.utils.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

        // (1) Locate tasks
        final ProcessingJobSubTask metaExtractionProcessingTask = locateMetaInfoExtractionProcessingTask(task);
        final ProcessingJobSubTask colorExtractionProcessingTask = locateColorExtractionProcessingTask(task);
        final List<ProcessingJobSubTask> thumbnailGenerationProcessingTasks = locateThumbnailExtractionProcessingTask(task);

        // (2) Execute tasks
        final MediaMetaInfoTuple mediaMetaInfoTuple = (metaExtractionProcessingTask != null) ? extractMetaInfo(originalFilePath, originalFileUrl, responseType, metaExtractionProcessingTask) : null;
        final ImageMetaInfo imageColorMetaInfo = (colorExtractionProcessingTask != null) ? extractColor(originalFilePath) : null;
        final Map<ProcessingJobSubTask, MediaFile> generatedThumbnails = generateThumbnails(originalFilePath, originalFileUrl, originalFileContent, processingProcessId, thumbnailGenerationProcessingTasks);


        // (3) Post task execution

        // (3.1) Insert the color palette in all the thumbnail meta infos if there is a color palette.
        if (imageColorMetaInfo != null) {
            for (final Map.Entry<ProcessingJobSubTask, MediaFile> thumbnailEntry : generatedThumbnails.entrySet()) {
                generatedThumbnails.put(thumbnailEntry.getKey(), thumbnailEntry.getValue().withColorPalette(imageColorMetaInfo.getColorPalette()));
            }
        }


        // (3.2) Persist result & cleanup
        for (final Map.Entry<ProcessingJobSubTask, MediaFile> thumbnailEntry : generatedThumbnails.entrySet()) {
            mediaStorageClient.createOrModify(thumbnailEntry.getValue());
        }
        Files.deleteIfExists(Paths.get(originalFilePath));

        return new ProcessingResultTuple(mediaMetaInfoTuple, generatedThumbnails.values(), imageColorMetaInfo);
    }

    private final List<ProcessingJobSubTask> locateThumbnailExtractionProcessingTask(final ProcessingJobTaskDocumentReference task) {
        List<ProcessingJobSubTask> results = new ArrayList();
        for (final ProcessingJobSubTask subTask : task.getProcessingTasks()) {
            if (subTask.getTaskType() == ProcessingJobSubTaskType.GENERATE_THUMBNAIL) {
                results.add(subTask);
            }
        }
        return results;
    }


    private final ProcessingJobSubTask locateColorExtractionProcessingTask(final ProcessingJobTaskDocumentReference task) {
        ProcessingJobSubTask result = null;
        for (final ProcessingJobSubTask subTask : task.getProcessingTasks()) {
            if (subTask.getTaskType() == ProcessingJobSubTaskType.COLOR_EXTRACTION) {
                if (result == null) {
                    result = subTask;
                } else {
                    throw new IllegalArgumentException("Cannot process configuration for document : " + task.getSourceDocumentReferenceID() + ". The configuration contains more than one color extraction subtask. Should be exactly one.");
                }
            }
        }
        return result;
    }


    private final ProcessingJobSubTask locateMetaInfoExtractionProcessingTask(final ProcessingJobTaskDocumentReference task) {
        ProcessingJobSubTask result = null;
        for (final ProcessingJobSubTask subTask : task.getProcessingTasks()) {
            if (subTask.getTaskType() == ProcessingJobSubTaskType.META_EXTRACTION) {
                if (result == null) {
                    result = subTask;
                } else {
                    throw new IllegalArgumentException("Cannot process configuration for document : " + task.getSourceDocumentReferenceID() + ". The configuration contains more than one meta extraction subtask. Should be exactly one.");
                }
            }
        }
        return result;
    }

    private final ImageMetaInfo extractColor(String originalFilePath) throws IOException, InterruptedException {
        if (MediaMetaDataUtils.classifyUrl(originalFilePath).equals(ContentType.IMAGE)) {
            return colorExtractor.colorExtraction(originalFilePath);
        }
        return null;
    }

    private final MediaMetaInfoTuple extractMetaInfo(final String originalFilePath, final String originalFileUrl, final ResponseType responseType, final ProcessingJobSubTask metaExtractionProcessingTask) throws Exception {
        if (responseType.equals(ResponseType.NO_STORAGE)) {
            throw new IllegalArgumentException("Configuration for url " + originalFileUrl + " for sub task " + metaExtractionProcessingTask.getConfig() + "Cannot execute meta info extraction because the media file is not stored.");
        }
        return metaInfoExtractor.extract(originalFilePath);
    }

    private final Map<ProcessingJobSubTask, MediaFile> generateThumbnails(final String originalFilePath, final String originalFileUrl, final byte[] originalFileContent, final String processingProcessId, final List<ProcessingJobSubTask> thumbnailGenerationProcessingTasks) throws Exception {
        final Map<ProcessingJobSubTask, MediaFile> results = new HashMap<ProcessingJobSubTask, MediaFile>();
        for (final ProcessingJobSubTask thumbnailGenerationTask : thumbnailGenerationProcessingTasks) {
            if (MediaMetaDataUtils.classifyUrl(originalFilePath).equals(ContentType.IMAGE)) {
                final GenericSubTaskConfiguration config = thumbnailGenerationTask.getConfig();
                results.put(thumbnailGenerationTask, thumbnailGenerator.createMediaFileWithThumbnail(config.getThumbnailConfig().getHeight(),
                        config.getThumbnailConfig().getWidth(), processingProcessId, originalFileUrl,
                        originalFileContent, originalFilePath));
            }
        }
        return results;
    }
}