package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;

/**
 * MongoDB DAO implementation for CRUD with source_document_reference_metainfo collection
 */
public class SourceDocumentReferenceMetaInfoDaoImpl implements SourceDocumentReferenceMetaInfoDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    public SourceDocumentReferenceMetaInfoDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public boolean create(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo, WriteConcern writeConcern) {
        if(read(sourceDocumentReferenceMetaInfo.getId()) == null) {
            datastore.save(sourceDocumentReferenceMetaInfo, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public SourceDocumentReferenceMetaInfo read(String id) {
        return datastore.get(SourceDocumentReferenceMetaInfo.class, id);
    }

    @Override
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo, WriteConcern writeConcern) {
        if(read(sourceDocumentReferenceMetaInfo.getId()) != null) {
            datastore.save(sourceDocumentReferenceMetaInfo, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(SourceDocumentReferenceMetaInfo.class, id);
    }

}
