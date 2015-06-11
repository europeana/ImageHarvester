package eu.europeana.harvester.cluster.slave.processing;

import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.slave.SlaveMetrics;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaDataUtils;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoTuple;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.sql.Ref;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlaveProcessor {

    private final MediaMetaInfoExtractor metaInfoExtractor;
    private final ThumbnailGenerator thumbnailGenerator;
    private final ColorExtractor colorExtractor;
    private final MediaStorageClient mediaStorageClient;


    public SlaveProcessor(MediaMetaInfoExtractor metaInfoExtractor, ThumbnailGenerator thumbnailGenerator, ColorExtractor colorExtractor,
                          MediaStorageClient mediaStorageClient) {
        this.metaInfoExtractor = metaInfoExtractor;
        this.thumbnailGenerator = thumbnailGenerator;
        this.colorExtractor = colorExtractor;
        this.mediaStorageClient = mediaStorageClient;

    }

    public ProcessingResultTuple process(final ProcessingJobTaskDocumentReference task, String originalFilePath, String originalFileUrl, byte[] originalFileContent,
                                         ResponseType responseType, ReferenceOwner referenceOwner) throws Exception {

        // (1) Locate tasks
        final ProcessingJobSubTask metaExtractionProcessingTask = locateMetaInfoExtractionProcessingTask(task);
        final ProcessingJobSubTask colorExtractionProcessingTask = locateColorExtractionProcessingTask(task);
        final List<ProcessingJobSubTask> thumbnailGenerationProcessingTasks = locateThumbnailExtractionProcessingTask(task);

        // (2) Execute tasks
        final MediaMetaInfoTuple mediaMetaInfoTuple = (metaExtractionProcessingTask != null) ? extractMetaInfo(originalFilePath, originalFileUrl, responseType, metaExtractionProcessingTask) : null;
        final ImageMetaInfo imageColorMetaInfo = (colorExtractionProcessingTask != null) ? extractColor(originalFilePath) : null;
        final Map<ProcessingJobSubTask, MediaFile> generatedThumbnails = generateThumbnails(originalFilePath, originalFileUrl, originalFileContent, referenceOwner, thumbnailGenerationProcessingTasks);


        // (3) Post task execution

        // (3.1) Insert the color palette in all the thumbnail meta infos if there is a color palette.
        if (imageColorMetaInfo != null) {
            for (final Map.Entry<ProcessingJobSubTask, MediaFile> thumbnailEntry : generatedThumbnails.entrySet()) {
                generatedThumbnails.put(thumbnailEntry.getKey(), thumbnailEntry.getValue().withColorPalette(imageColorMetaInfo.getColorPalette()));
            }
        }

        // (3.2) Persist thumbnails & cleanup
        SlaveMetrics.Worker.Slave.Processing.thumbnailStorageCounter.inc();
        final Timer.Context thumbnailStorageDurationContext = SlaveMetrics.Worker.Slave.Processing.thumbnailStorageDuration.time();

        try {
            for (final Map.Entry<ProcessingJobSubTask, MediaFile> thumbnailEntry : generatedThumbnails.entrySet()) {
                mediaStorageClient.createOrModify(thumbnailEntry.getValue());
            }
        } finally {
            thumbnailStorageDurationContext.stop();

            // (3.3) Cache original if it is an image
            if (MediaMetaDataUtils.classifyUrl(originalFilePath).equals(ContentType.IMAGE) && mediaMetaInfoTuple != null) {
                SlaveMetrics.Worker.Slave.Processing.originalCachingCounter.inc();
                final Timer.Context originalCachingDurationContext = SlaveMetrics.Worker.Slave.Processing.originalCachingDuration.time();
                try {
                    final MediaFile mediaFile = generateOriginal(originalFilePath, originalFileUrl, originalFileContent, referenceOwner, mediaMetaInfoTuple.getImageMetaInfo());
                    mediaStorageClient.createOrModify(mediaFile);
                } finally {
                    originalCachingDurationContext.stop();
                    Files.deleteIfExists(Paths.get(originalFilePath));
                }
            } else {
                Files.deleteIfExists(Paths.get(originalFilePath));
            }

        }


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
            SlaveMetrics.Worker.Slave.Processing.colorExtractionCounter.inc();
            final Timer.Context colorExtractionDurationContext = SlaveMetrics.Worker.Slave.Processing.colorExtractionDuration.time();
            try {
                return colorExtractor.colorExtraction(originalFilePath);
            } finally {
                colorExtractionDurationContext.stop();
            }
        }
        return null;
    }

    private final MediaMetaInfoTuple extractMetaInfo(final String originalFilePath, final String originalFileUrl, final ResponseType responseType, final ProcessingJobSubTask metaExtractionProcessingTask) throws Exception {
        if (responseType.equals(ResponseType.NO_STORAGE)) {
            throw new IllegalArgumentException("Configuration for url " + originalFileUrl + " for sub task " + metaExtractionProcessingTask.getConfig() + "Cannot execute meta info extraction because the media file is not stored.");
        }
        SlaveMetrics.Worker.Slave.Processing.metaInfoExtractionCounter.inc();
        final Timer.Context metaInfoExtractionDurationContext = SlaveMetrics.Worker.Slave.Processing.metaInfoExtractionDuration.time();
        try {
            return metaInfoExtractor.extract(originalFilePath);
        } finally {
            metaInfoExtractionDurationContext.stop();
        }
    }

    private final MediaFile generateOriginal(final String originalFilePath, final String originalFileUrl, final byte[] originalFileContent, final ReferenceOwner referenceOwner, final ImageMetaInfo imageMetaInfo) throws NoSuchAlgorithmException {

        if (originalFilePath == null || originalFileUrl == null || originalFileContent == null)
            throw new IllegalArgumentException("Cannot generate media file as all must be non-null : filepath, url & content");
        if (imageMetaInfo == null)
            throw new IllegalArgumentException("Cannot generate media file from null image meta info");

        return MediaFile.createMinimalMediaFileWithSizeType("ORIGINAL", referenceOwner.getExecutionId(), originalFilePath,
                originalFileUrl, DateTime.now(), originalFileContent, imageMetaInfo.getMimeType(),
                originalFileContent.length);

    }

    private final Map<ProcessingJobSubTask, MediaFile> generateThumbnails(final String originalFilePath, final String originalFileUrl, final byte[] originalFileContent, final ReferenceOwner referenceOwner, final List<ProcessingJobSubTask> thumbnailGenerationProcessingTasks) throws Exception {
        final Map<ProcessingJobSubTask, MediaFile> results = new HashMap<ProcessingJobSubTask, MediaFile>();
        for (final ProcessingJobSubTask thumbnailGenerationTask : thumbnailGenerationProcessingTasks) {
            if (MediaMetaDataUtils.classifyUrl(originalFilePath).equals(ContentType.IMAGE)) {
                SlaveMetrics.Worker.Slave.Processing.thumbnailGenerationCounter.inc();
                final Timer.Context thumbnailGenerationDurationContext = SlaveMetrics.Worker.Slave.Processing.thumbnailGenerationDuration.time();
                try {
                    final GenericSubTaskConfiguration config = thumbnailGenerationTask.getConfig();
                    final MediaFile thumbnailMediaFile = thumbnailGenerator.createMediaFileWithThumbnail(config.getThumbnailConfig().getHeight(),
                            config.getThumbnailConfig().getWidth(), referenceOwner.getExecutionId(), originalFileUrl,
                            originalFileContent, originalFilePath);
                    results.put(thumbnailGenerationTask, thumbnailMediaFile);
                } finally {
                    thumbnailGenerationDurationContext.stop();
                }
            }
        }
        return results;
    }
}