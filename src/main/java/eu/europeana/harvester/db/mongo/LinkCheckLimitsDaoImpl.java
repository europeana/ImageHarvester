package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import eu.europeana.harvester.db.LinkCheckLimitsDao;
import eu.europeana.harvester.domain.LinkCheckLimits;

import java.util.List;

public class LinkCheckLimitsDaoImpl implements LinkCheckLimitsDao {

    private final Datastore datastore;

    public LinkCheckLimitsDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public void create(LinkCheckLimits linkCheckLimit) {
        datastore.save(linkCheckLimit);
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
    public boolean update(LinkCheckLimits linkCheckLimit) {
        Query<LinkCheckLimits> query = datastore.find(LinkCheckLimits.class);
        query.criteria("id").equal(linkCheckLimit.getId());

        List<LinkCheckLimits> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(query);
            datastore.save(linkCheckLimit);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(LinkCheckLimits linkCheckLimit) {
        Query<LinkCheckLimits> query = datastore.find(LinkCheckLimits.class);
        query.criteria("id").equal(linkCheckLimit.getId());

        List<LinkCheckLimits> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(linkCheckLimit);

            return true;
        }

        return false;
    }

    @Override
    public void createOrModify(LinkCheckLimits linkCheckLimit) {
        if(!update(linkCheckLimit)) {
            create(linkCheckLimit);
        }
    }

}
