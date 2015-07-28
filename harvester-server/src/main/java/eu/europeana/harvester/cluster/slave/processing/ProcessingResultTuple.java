package eu.europeana.harvester.cluster.slave.processing;

import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoTuple;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.MediaFile;

import java.util.Collection;

public class ProcessingResultTuple {
    private final MediaMetaInfoTuple mediaMetaInfoTuple;
    private final Collection<MediaFile> generatedThumbnails;
    private final ImageMetaInfo imageColorMetaInfo;
    private final DocumentReferenceTaskType documentReferenceTaskType;

    public ProcessingResultTuple(DocumentReferenceTaskType documentReferenceTaskType,
                                 MediaMetaInfoTuple mediaMetaInfoTuple,
                                 Collection<MediaFile> generatedThumbnails,
                                 ImageMetaInfo imageColorMetaInfo) {
        this.documentReferenceTaskType = documentReferenceTaskType;
        this.mediaMetaInfoTuple = mediaMetaInfoTuple;
        this.generatedThumbnails = generatedThumbnails;
        this.imageColorMetaInfo = imageColorMetaInfo;
    }

    public MediaMetaInfoTuple getMediaMetaInfoTuple() {
        return mediaMetaInfoTuple;
    }

    public Collection<MediaFile> getGeneratedThumbnails() {
        return generatedThumbnails;
    }

    public ImageMetaInfo getImageColorMetaInfo() {
        return imageColorMetaInfo;
    }

    public DocumentReferenceTaskType getDocumentReferenceTaskType () {
        return documentReferenceTaskType;
    }
}
