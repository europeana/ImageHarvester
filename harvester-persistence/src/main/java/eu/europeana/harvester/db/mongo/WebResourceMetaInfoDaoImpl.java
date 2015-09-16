package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.query.Query;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import com.mongodb.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with WebResourceMetaInfo collection
 */
public class WebResourceMetaInfoDaoImpl implements WebResourceMetaInfoDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    private final Morphia morphia;

    private final DB mongoDB;
    public WebResourceMetaInfoDaoImpl(DB mongoDB,Morphia morphia,Datastore datastore) {
        this.mongoDB = mongoDB;
        this.morphia = morphia;
        this.datastore = datastore;
    }

    @Override
    public boolean create(WebResourceMetaInfo webResourceMetaInfo, WriteConcern writeConcern) {
        if(read(webResourceMetaInfo.getId()) == null) {
            datastore.save(webResourceMetaInfo, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int createOrModify(Collection<WebResourceMetaInfo> webResourceMetaInfos, WriteConcern writeConcern) {
        if (webResourceMetaInfos == null ) return 0;
        if (webResourceMetaInfos.isEmpty() ) return 0;
        final BulkWriteOperation bulk = mongoDB.getCollection(datastore.getCollection(WebResourceMetaInfo.class).getName()).initializeUnorderedBulkOperation();

        for (final WebResourceMetaInfo one : webResourceMetaInfos) {
            BulkWriteRequestBuilder bulkWriteRequestBuilder=bulk.find(new BasicDBObject("_id",one.getId()));
            if (bulkWriteRequestBuilder != null){
                BulkUpdateRequestBuilder updateReq = bulkWriteRequestBuilder.upsert();
                updateReq.replaceOne(morphia.toDBObject(one));
            } else {
                bulk.insert(morphia.toDBObject(one));
            }
        }
        final BulkWriteResult result = bulk.execute();
        return result.getInsertedCount()+result.getModifiedCount();
    }

    @Override
    public WebResourceMetaInfo read(String id) {
        return datastore.get(WebResourceMetaInfo.class, id);
    }

    @Override
    public List<WebResourceMetaInfo> read(List<String> ids) {
        final Query<WebResourceMetaInfo> query = datastore.createQuery(WebResourceMetaInfo.class).field("_id").hasAnyOf(ids);
        if(query == null) {return new ArrayList<>();}

        return query.asList();
    }

    @Override
    public boolean update(WebResourceMetaInfo webResourceMetaInfo, WriteConcern writeConcern) {
        if(read(webResourceMetaInfo.getId()) != null) {
            datastore.save(webResourceMetaInfo, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(WebResourceMetaInfo.class, id);
    }
}
