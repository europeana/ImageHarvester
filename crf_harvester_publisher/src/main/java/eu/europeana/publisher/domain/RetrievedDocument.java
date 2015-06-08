package eu.europeana.publisher.domain;

import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;

/**
 * Created by salexandru on 08.06.2015.
 */
public class RetrievedDocument {

    private final DocumentStatistic documentStatistic;
    private final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo;


    public RetrievedDocument (DocumentStatistic documentStatistic,
                              SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo) {
        this.documentStatistic = documentStatistic;
        this.sourceDocumentReferenceMetaInfo = sourceDocumentReferenceMetaInfo;
    }

    public DocumentStatistic getDocumentStatistic () {
        return documentStatistic;
    }

    public SourceDocumentReferenceMetaInfo getSourceDocumentReferenceMetaInfo () {
        return sourceDocumentReferenceMetaInfo;
    }
}
