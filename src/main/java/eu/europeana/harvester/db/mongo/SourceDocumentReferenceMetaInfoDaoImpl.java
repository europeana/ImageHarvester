package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;

import java.util.List;

public class SourceDocumentReferenceMetaInfoDaoImpl implements SourceDocumentReferenceMetaInfoDao {

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
        Query<SourceDocumentReferenceMetaInfo> query = datastore.find(SourceDocumentReferenceMetaInfo.class);
        query.criteria("id").equal(id);

        List<SourceDocumentReferenceMetaInfo> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

    @Override
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo, WriteConcern writeConcern) {
        Query<SourceDocumentReferenceMetaInfo> query = datastore.find(SourceDocumentReferenceMetaInfo.class);
        query.criteria("id").equal(sourceDocumentReferenceMetaInfo.getId());

        List<SourceDocumentReferenceMetaInfo> result = query.asList();
        if(!result.isEmpty()) {
            datastore.save(sourceDocumentReferenceMetaInfo, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo, WriteConcern writeConcern) {
        Query<SourceDocumentReferenceMetaInfo> query = datastore.find(SourceDocumentReferenceMetaInfo.class);
        query.criteria("id").equal(sourceDocumentReferenceMetaInfo.getId());

        List<SourceDocumentReferenceMetaInfo> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(sourceDocumentReferenceMetaInfo, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public SourceDocumentReferenceMetaInfo findBySourceDocumentReferenceId(String id) {
        Query<SourceDocumentReferenceMetaInfo> query = datastore.find(SourceDocumentReferenceMetaInfo.class);
        query.criteria("sourceDocumentReferenceId").equal(id);

        List<SourceDocumentReferenceMetaInfo> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }

        return null;
    }

}
