package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.query.Query;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import com.mongodb.*;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with WebResourceMetaInfo collection
 */
public class WebResourceMetaInfoDaoImpl implements WebResourceMetaInfoDao {
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

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

        // Step 1 : Find out which meta info's are already there
        final List<String> candiateIds = new ArrayList<String>();
        for (WebResourceMetaInfo one : webResourceMetaInfos ) {
            candiateIds.add(one.getId());
        }

        final BasicDBObject query = new BasicDBObject();
        final BasicDBObject retrievedFields = new BasicDBObject();
        retrievedFields.put("_id", 1);
        query.put("_id", new BasicDBObject("$in", candiateIds));
        DBCursor existing = mongoDB.getCollection(datastore.getCollection(WebResourceMetaInfo.class).getName())
                .find(query, retrievedFields);

        final List<String> existingIds = new ArrayList<String>();
        while (existing.hasNext()) {
            final DBObject nextOne = existing.next();
            existingIds.add(nextOne.get("_id").toString());
        }

        LOG.debug("WebResourceMetaInfo {} to be created out of total of {}",existingIds.size(),webResourceMetaInfos.size());

        // Step 2 : update or create
        final BulkWriteOperation bulk = mongoDB.getCollection(datastore.getCollection(WebResourceMetaInfo.class).getName()).initializeOrderedBulkOperation();

        for (final WebResourceMetaInfo one : webResourceMetaInfos) {
           if (existingIds.contains(one.getId())) {
               bulk.find(new BasicDBObject("_id",one.getId())).upsert().replaceOne(morphia.toDBObject(one));
           } else {
               bulk.insert(morphia.toDBObject(one));
           }
        }

        // Step 3 : execute bulk
        final BulkWriteResult result = bulk.execute(writeConcern);

        return webResourceMetaInfos.size();
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
