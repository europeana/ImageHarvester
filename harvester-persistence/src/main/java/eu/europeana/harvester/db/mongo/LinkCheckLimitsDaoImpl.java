package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.LinkCheckLimitsDao;
import eu.europeana.harvester.domain.LinkCheckLimits;

/**
 * MongoDB DAO implementation for CRUD with link_check_limits collection
 */
public class LinkCheckLimitsDaoImpl implements LinkCheckLimitsDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    public LinkCheckLimitsDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public boolean create(LinkCheckLimits linkCheckLimits, WriteConcern writeConcern) {
        if(read(linkCheckLimits.getId()) == null) {
            datastore.save(linkCheckLimits, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void createOrModify(LinkCheckLimits linkCheckLimit, WriteConcern writeConcern) {
        datastore.save(linkCheckLimit, writeConcern);
    }

    @Override
    public LinkCheckLimits read(String id) {
        return datastore.get(LinkCheckLimits.class, id);
    }

    @Override
    public boolean update(LinkCheckLimits linkCheckLimit, WriteConcern writeConcern) {
        if(read(linkCheckLimit.getId()) != null) {
            datastore.save(linkCheckLimit, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(LinkCheckLimits.class, id);
    }

}
