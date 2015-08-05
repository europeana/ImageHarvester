package eu.europeana.harvester.cluster.slave.processing;

import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.slave.SlaveMetrics;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.exceptiions.ColorExtractionException;
import eu.europeana.harvester.cluster.slave.processing.exceptiions.MetaInfoExtractionException;
import eu.europeana.harvester.cluster.slave.processing.exceptiions.ThumbnailGenerationException;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaDataUtils;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoTuple;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlaveProcessor {
    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

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

    public ProcessingResultTuple process(final ProcessingJobTaskDocumentReference task,
                                         String originalFilePath,
                                         String originalFileUrl,
                                         byte[] originalFileContent,
                                         ResponseType responseType,
                                         ReferenceOwner referenceOwner) {

        // (1) Locate tasks
        final ProcessingJobSubTask metaExtractionProcessingTask = locateMetaInfoExtractionProcessingTask(task);
        final ProcessingJobSubTask colorExtractionProcessingTask = locateColorExtractionProcessingTask(task);
        final List<ProcessingJobSubTask> thumbnailGenerationProcessingTasks = locateThumbnailExtractionProcessingTask(task);

        ProcessingJobSubTaskStats stats = new ProcessingJobSubTaskStats();

        // (2) Execute tasks
        MediaMetaInfoTuple mediaMetaInfoTuple = null;
        ImageMetaInfo imageColorMetaInfo = null;
        Map<ProcessingJobSubTask, MediaFile> generatedThumbnails = new HashMap<>();

        // Meta data extraction
        if (null != metaExtractionProcessingTask) {
            try {
                mediaMetaInfoTuple = extractMetaInfo(originalFilePath, originalFileUrl, responseType,
                        metaExtractionProcessingTask);

                if (null != mediaMetaInfoTuple && mediaMetaInfoTuple.isValid()) {
                    stats = stats.withMetaExtractionState(ProcessingJobSubTaskState.SUCCESS);
                } else {
                    stats = stats.withMetaExtractionState(ProcessingJobSubTaskState.FAILED);
                }
            } catch (Exception e) {
                stats = stats.withMetaExtractionState(ProcessingJobSubTaskState.ERROR, e);
            }
        }

        // Color extraction
        if (null != colorExtractionProcessingTask) {
            try {
                imageColorMetaInfo = extractColor(originalFilePath);

                if (null != imageColorMetaInfo && null != imageColorMetaInfo.getColorPalette() &&
                        imageColorMetaInfo.getColorPalette().length > 0) {
                    stats = stats.withColorExtractionState(ProcessingJobSubTaskState.SUCCESS);
                } else {
                    stats = stats.withColorExtractionState(ProcessingJobSubTaskState.FAILED);
                }
            } catch (Exception e) {
                stats = stats.withColorExtractionState(ProcessingJobSubTaskState.ERROR, e);
            }
        }

        // Thumbnail generation
        if (null != thumbnailGenerationProcessingTasks && !thumbnailGenerationProcessingTasks.isEmpty()) {
            try {
                generatedThumbnails = generateThumbnails(originalFilePath, originalFileUrl, originalFileContent,
                        referenceOwner, thumbnailGenerationProcessingTasks);

                if (null != generatedThumbnails && generatedThumbnails.size() == thumbnailGenerationProcessingTasks.size()) {
                    stats = stats.withThumbnailGenerationState(ProcessingJobSubTaskState.SUCCESS);
                } else {
                    stats = stats.withThumbnailGenerationState(ProcessingJobSubTaskState.FAILED);
                }
            } catch (Exception e) {
                stats = stats.withThumbnailGenerationState(ProcessingJobSubTaskState.ERROR, e);
            }
        }


        // (3) Post task execution

        // (3.1) Insert the color palette in all the thumbnail meta infos if there is a color palette.
        if (imageColorMetaInfo != null)

        {
            for (final Map.Entry<ProcessingJobSubTask, MediaFile> thumbnailEntry : generatedThumbnails.entrySet()) {
                generatedThumbnails.put(thumbnailEntry.getKey(), thumbnailEntry.getValue().withColorPalette(imageColorMetaInfo.getColorPalette()));
            }
        }

        // (3.2) Persist thumbnails & cleanup
        SlaveMetrics.Worker.Slave.Processing.thumbnailStorageCounter.inc();
        final Timer.Context thumbnailStorageDurationContext = SlaveMetrics.Worker.Slave.Processing.thumbnailStorageDuration.time();

        try

        {
            for (final Map.Entry<ProcessingJobSubTask, MediaFile> thumbnailEntry : generatedThumbnails.entrySet()) {
                mediaStorageClient.createOrModify(thumbnailEntry.getValue());
            }
            if (null == generatedThumbnails || generatedThumbnails.isEmpty()) {
                stats = stats.withThumbnailStorageState(ProcessingJobSubTaskState.NEVER_EXECUTED);
            }
            else {
                stats = stats.withThumbnailStorageState(ProcessingJobSubTaskState.SUCCESS);
            }
        } catch (Exception e) {
            stats = stats.withThumbnailStorageState(ProcessingJobSubTaskState.ERROR, e);
        } finally {
            thumbnailStorageDurationContext.stop();
            try {
                cacheOriginalImage(originalFilePath, originalFileUrl, originalFileContent, referenceOwner,
                        mediaMetaInfoTuple);

            } catch (Exception e) {
                stats = stats.withThumbnailStorageState(ProcessingJobSubTaskState.ERROR, e);
            }
        }

        return new ProcessingResultTuple(stats,
                                         mediaMetaInfoTuple,
                                         generatedThumbnails.values(),
                                         imageColorMetaInfo
        );
    }

    private void cacheOriginalImage(String originalFilePath, String originalFileUrl, byte[] originalFileContent,
                                    ReferenceOwner referenceOwner, MediaMetaInfoTuple mediaMetaInfoTuple) throws
            NoSuchAlgorithmException,
            IOException {// (3.3) Cache original if it is an image
        if (MediaMetaDataUtils.classifyUrl(originalFilePath).equals(ContentType.IMAGE) && mediaMetaInfoTuple != null) {
            SlaveMetrics.Worker.Slave.Processing.originalCachingCounter.inc();
            final Timer.Context originalCachingDurationContext = SlaveMetrics.Worker.Slave.Processing.originalCachingDuration
                    .time();
            try {
                final MediaFile mediaFile = generateOriginal(originalFilePath, originalFileUrl,
                        originalFileContent, referenceOwner,
                        mediaMetaInfoTuple.getImageMetaInfo());
                mediaStorageClient.createOrModify(mediaFile);
            } finally {
                originalCachingDurationContext.stop();
                Files.deleteIfExists(Paths.get(originalFilePath));
            }
        } else {
            Files.deleteIfExists(Paths.get(originalFilePath));
        }
    }

    private final List<ProcessingJobSubTask> locateThumbnailExtractionProcessingTask(final ProcessingJobTaskDocumentReference task) {
        List<ProcessingJobSubTask> results = new ArrayList();
        for (final ProcessingJobSubTask subTask : task.getProcessingTasks()) {
            if (subTask.getTaskType() == ProcessingJobSubTaskType.GENERATE_THUMBNAIL && subTask.getConfig().getThumbnailConfig().getHeight() != 180) {
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

    private final ImageMetaInfo extractColor(String originalFilePath) throws ColorExtractionException {
        if (MediaMetaDataUtils.classifyUrl(originalFilePath).equals(ContentType.IMAGE)) {
            SlaveMetrics.Worker.Slave.Processing.colorExtractionCounter.inc();
            final Timer.Context colorExtractionDurationContext = SlaveMetrics.Worker.Slave.Processing.colorExtractionDuration.time();
            try {
                return colorExtractor.colorExtraction(originalFilePath);
            } catch (Exception e) {
                throw new ColorExtractionException(e);
            } finally {
                colorExtractionDurationContext.stop();
            }
        }
        return null;
    }

    private final MediaMetaInfoTuple extractMetaInfo(final String originalFilePath,
                                                     final String originalFileUrl,
                                                     final ResponseType responseType,
                                                     final ProcessingJobSubTask metaExtractionProcessingTask) throws MetaInfoExtractionException {
        if (responseType.equals(ResponseType.NO_STORAGE)) {
            throw new IllegalArgumentException("Configuration for url " + originalFileUrl + " for sub task " + metaExtractionProcessingTask.getConfig() + "Cannot execute meta info extraction because the media file is not stored.");
        }
        SlaveMetrics.Worker.Slave.Processing.metaInfoExtractionCounter.inc();
        final Timer.Context metaInfoExtractionDurationContext = SlaveMetrics.Worker.Slave.Processing.metaInfoExtractionDuration.time();
        try {
            return metaInfoExtractor.extract(originalFilePath);
        } catch (Exception e) {
            throw new MetaInfoExtractionException(e);
        } finally {
            metaInfoExtractionDurationContext.stop();
        }
    }

    private final MediaFile generateOriginal(final String originalFilePath, final String originalFileUrl, final byte[] originalFileContent, final ReferenceOwner referenceOwner, final ImageMetaInfo imageMetaInfo) throws NoSuchAlgorithmException {

        if (originalFilePath == null || originalFileUrl == null || originalFileContent == null)
            throw new IllegalArgumentException("Cannot generate media file as all must be non-null : file path, url & content");
        if (imageMetaInfo == null)
            throw new IllegalArgumentException("Cannot generate media file from null image meta info");

        return MediaFile.createMinimalMediaFileWithSizeType("ORIGINAL", referenceOwner.getExecutionId(), originalFilePath,
                originalFileUrl, DateTime.now(), originalFileContent, imageMetaInfo.getMimeType(),
                originalFileContent.length);

    }

    private final Map<ProcessingJobSubTask, MediaFile> generateThumbnails(final String originalFilePath,
                                                                          final String originalFileUrl,
                                                                          final byte[] originalFileContent,
                                                                          final ReferenceOwner referenceOwner,
                                                                          final List<ProcessingJobSubTask> thumbnailGenerationProcessingTasks) throws ThumbnailGenerationException {
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
                } catch (Exception e) {
                    throw new ThumbnailGenerationException(e);
                } finally {
                    thumbnailGenerationDurationContext.stop();
                }
            }
        }
        return results;
    }
}