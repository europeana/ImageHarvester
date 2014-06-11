package eu.europeana.harvester.db;


import eu.europeana.harvester.domain.LinkCheckLimits;

/**
 * DAO for CRUD with link_check_limit collection
 */
public interface LinkCheckLimitsDao {

    /**
     * Persists a LinkCheckLimits object
     * @param linkCheckLimit - a new object
     */
    public void create(LinkCheckLimits linkCheckLimit);

    /**
     * Reads and returns a LinkCheckLimits object
     * @param id the unique id of the record
     * @return - found LinkCheckLimits object, it can be null
     */
    public LinkCheckLimits read(String id);

    /**
     * Updates a LinkCheckLimits record
     * @param linkCheckLimit the modified LinkCheckLimits object
     * @return - success or failure
     */
    public boolean update(LinkCheckLimits linkCheckLimit);

    /**
     * Deletes a record from DB
     * @param linkCheckLimit the unnecessary object
     * @return - success or failure
     */
    public boolean delete(LinkCheckLimits linkCheckLimit);

    /**
     * Modifies an existing LinkCheckLimits record, if it doesn't exists then creates it.
     * @param linkCheckLimit modified or new object
     */
    public void createOrModify(LinkCheckLimits linkCheckLimit);

}
