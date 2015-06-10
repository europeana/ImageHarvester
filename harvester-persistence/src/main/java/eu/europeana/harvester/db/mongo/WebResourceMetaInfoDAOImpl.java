package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.WebResourceMetaInfoDAO;
import eu.europeana.harvester.domain.WebResourceMetaInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with WebResourceMetaInfo collection
 */
public class WebResourceMetaInfoDAOImpl implements WebResourceMetaInfoDAO {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    public WebResourceMetaInfoDAOImpl (Datastore datastore) {
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
    public Iterable<com.google.code.morphia.Key<WebResourceMetaInfo>> createOrModify(Collection<WebResourceMetaInfo> webResourceMetaInfos, WriteConcern writeConcern) {
        return datastore.save(webResourceMetaInfos, writeConcern);
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
