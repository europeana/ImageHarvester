package eu.europeana.publisher.domain;

import eu.europeana.harvester.domain.ProcessingJobSubTaskState;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.harvester.domain.URLSourceType;

import java.util.*;

/**
 * Created by salexandru on 24.08.2015.
 */
public class HarvesterRecord {
    private ReferenceOwner owner = null;

    private final HarvesterDocument edmObjectDocument;
    private final HarvesterDocument edmIsShownByDocument;
    private final HarvesterDocument edmIsShownAtDocument;
    private final List<HarvesterDocument> edmHasViewDocuments;

    public HarvesterRecord() {
        edmHasViewDocuments = null;
        edmObjectDocument = null;
        edmIsShownAtDocument = null;
        edmIsShownByDocument = null;
        owner = null;
    }

    public HarvesterRecord (HarvesterDocument edmObjectDocument,
                            HarvesterDocument edmIsShownByDocument,
                            HarvesterDocument edmIsShownAtDocument,
                            List<HarvesterDocument> edmHasViewDocuments) {

        this.edmObjectDocument = edmObjectDocument;
        this.edmIsShownByDocument = edmIsShownByDocument;
        this.edmIsShownAtDocument = edmIsShownAtDocument;
        this.edmHasViewDocuments = Collections.unmodifiableList(null == edmHasViewDocuments ?
                                                                new ArrayList<HarvesterDocument>() :
                                                                edmHasViewDocuments
                                                               );

        if (null != edmObjectDocument) owner = edmObjectDocument.getReferenceOwner();
        else if (null != edmIsShownAtDocument) owner = edmIsShownAtDocument.getReferenceOwner();
        else if (null != edmIsShownByDocument) owner = edmIsShownByDocument.getReferenceOwner();
        else if (null != edmHasViewDocuments && !edmHasViewDocuments.isEmpty()) {
            for (final HarvesterDocument document: edmHasViewDocuments) {
                if (null != document) {
                    owner = document.getReferenceOwner();
                    break;
                }
            }
        }
        else owner = null;
    }


    public String getRecordId() {
       return owner.getRecordId();
    }

    public List<HarvesterDocument> getAllDocuments() {
        final List<HarvesterDocument> allDocuments = new ArrayList<>();

        if (null != getEdmObjectDocument()) allDocuments.add(edmObjectDocument);
        if (null != getEdmIsShownByDocument()) allDocuments.add(edmIsShownByDocument);
        if (null != getEdmIsShownAtDocument()) allDocuments.add(edmIsShownAtDocument);

        for (final HarvesterDocument document: edmHasViewDocuments) {
            if (null != document) allDocuments.add(document);
        }

        return allDocuments;
    }

    public boolean updateEdmObject() {
        return
          null != getEdmIsShownByDocument() &&
          ProcessingJobSubTaskState.SUCCESS.equals(getEdmIsShownByDocument().getSubTaskStats().getThumbnailGenerationState()) &&
          ProcessingJobSubTaskState.SUCCESS.equals(getEdmIsShownByDocument().getSubTaskStats().getThumbnailStorageState());
    }

    public String newEdmObjectUrl() {
        if (updateEdmObject()) return getEdmIsShownByDocument().getUrl();
        return null == getEdmObjectDocument() ? null : getEdmObjectDocument().getUrl();
    }

    public ReferenceOwner getReferenceOwner() {return owner;}

    public Collection<SourceDocumentReferenceMetaInfo> getUniqueMetainfos() {
        final Map<String, SourceDocumentReferenceMetaInfo> metainfos = new HashMap<>();

        for (final HarvesterDocument document: getAllDocuments()) {
            if (null != document &&
                ProcessingJobSubTaskState.SUCCESS.equals(document.getSubTaskStats().getMetaExtractionState()) &&
                null != document.getSourceDocumentReferenceMetaInfo()) {
                if (URLSourceType.OBJECT.equals(document.getUrlSourceType())) continue;
                metainfos.put(document.getSourceDocumentReferenceId(), document.getSourceDocumentReferenceMetaInfo());
            }
        }

        return metainfos.values();
    }

    public HarvesterRecord with (URLSourceType type, HarvesterDocument ... document) {
        if (null == document || 0 == document.length) {
            throw new IllegalArgumentException("At least one document should be provided");
        }

        switch (type) {
            case HASVIEW: return withHasViewDocuments(Arrays.asList(document));

            case ISSHOWNAT:
                assert (1 == document.length);
                return withEdmIsShownAtDocument(document[0]);

            case ISSHOWNBY:
                assert (1 == document.length);
                return withEdmIsShownByDocument(document[0]);

            case OBJECT:
                assert (1 == document.length);
                return withEdmObjectDocument(document[0]);

            default:
                throw new IllegalArgumentException("Unkown UrlSourceType: " + type);
        }
    }

    public HarvesterRecord withEdmObjectDocument (HarvesterDocument document) {
        return new HarvesterRecord(document,
                                   this.getEdmIsShownByDocument(),
                                   this.getEdmIsShownAtDocument(),
                                   this.getEdmHasViewDocuments()
                                  );
    }

    public HarvesterRecord withEdmIsShownByDocument (HarvesterDocument document) {
        return new HarvesterRecord(this.getEdmObjectDocument(),
                                   document,
                                   this.getEdmIsShownAtDocument(),
                                   this.getEdmHasViewDocuments()
        );
    }

    public HarvesterRecord withEdmIsShownAtDocument (HarvesterDocument document) {
        return new HarvesterRecord(this.getEdmObjectDocument(),
                                   this.getEdmIsShownByDocument(),
                                   document,
                                   this.getEdmHasViewDocuments()
        );
    }

    public HarvesterRecord withHasViewDocuments (List<HarvesterDocument> document) {
        return new HarvesterRecord(this.getEdmObjectDocument(),
                                   this.getEdmIsShownByDocument(),
                                   this.getEdmIsShownAtDocument(),
                                   document
                                 );
    }

    public HarvesterDocument getEdmObjectDocument () {
        return edmObjectDocument;
    }

    public HarvesterDocument getEdmIsShownByDocument () {
        return edmIsShownByDocument;
    }

    public HarvesterDocument getEdmIsShownAtDocument () {
        return edmIsShownAtDocument;
    }

    public List<HarvesterDocument> getEdmHasViewDocuments () {
        return edmHasViewDocuments;
    }
}
