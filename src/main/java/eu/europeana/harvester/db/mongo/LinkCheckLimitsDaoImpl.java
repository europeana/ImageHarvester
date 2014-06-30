package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.LinkCheckLimitsDao;
import eu.europeana.harvester.domain.LinkCheckLimits;

import java.util.List;

public class LinkCheckLimitsDaoImpl implements LinkCheckLimitsDao {

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
        Query<LinkCheckLimits> query = datastore.find(LinkCheckLimits.class);
        query.criteria("id").equal(id);

        List<LinkCheckLimits> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

    @Override
    public boolean update(LinkCheckLimits linkCheckLimit, WriteConcern writeConcern) {
        Query<LinkCheckLimits> query = datastore.find(LinkCheckLimits.class);
        query.criteria("id").equal(linkCheckLimit.getId());

        List<LinkCheckLimits> result = query.asList();
        if(!result.isEmpty()) {
            datastore.save(linkCheckLimit);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(LinkCheckLimits linkCheckLimit, WriteConcern writeConcern) {
        Query<LinkCheckLimits> query = datastore.find(LinkCheckLimits.class);
        query.criteria("id").equal(linkCheckLimit.getId());

        List<LinkCheckLimits> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(linkCheckLimit);

            return true;
        }

        return false;
    }

}
