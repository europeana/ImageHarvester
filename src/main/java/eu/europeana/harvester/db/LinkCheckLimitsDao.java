package eu.europeana.harvester.db;


import eu.europeana.harvester.domain.LinkCheckLimits;

public interface LinkCheckLimitsDao {

    public void create(LinkCheckLimits linkCheckLimit);

    public LinkCheckLimits read(String id);

    public boolean update(LinkCheckLimits linkCheckLimit);

    public boolean delete(LinkCheckLimits linkCheckLimit);

}
