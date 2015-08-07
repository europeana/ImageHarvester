package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with SourceDocumentReferenceMetaInfo collection
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
    public Long getCount() {
        return datastore.getCount(SourceDocumentReferenceMetaInfo.class);
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
    public Iterable<com.google.code.morphia.Key<SourceDocumentReferenceMetaInfo>> createOrModify(Collection<SourceDocumentReferenceMetaInfo> sourceDocumentReferenceMetaInfos, WriteConcern writeConcern) {
        if (null == sourceDocumentReferenceMetaInfos || sourceDocumentReferenceMetaInfos.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        return datastore.save(sourceDocumentReferenceMetaInfos, writeConcern);
    }

    @Override
    public SourceDocumentReferenceMetaInfo read(String id) {
        return datastore.get(SourceDocumentReferenceMetaInfo.class, id);
    }

    @Override
    public List<SourceDocumentReferenceMetaInfo> read(Collection<String> ids) {

        final Query<SourceDocumentReferenceMetaInfo> query = datastore.createQuery(SourceDocumentReferenceMetaInfo.class).retrievedFields(true,"id","imageMetaInfo","audioMetaInfo","videoMetaInfo","textMetaInfo").hintIndex("_id_").field("_id").hasAnyOf(ids);
        if(query == null) {return new ArrayList<>();}

        return query.asList();
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
