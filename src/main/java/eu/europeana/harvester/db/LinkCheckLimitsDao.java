package eu.europeana.harvester.db;


import com.mongodb.WriteConcern;
import eu.europeana.harvester.domain.LinkCheckLimits;

/**
 * DAO for CRUD with link_check_limit collection
 */
public interface LinkCheckLimitsDao {

    /**
     * Persists a LinkCheckLimits object
     * @param linkCheckLimit - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    public boolean create(LinkCheckLimits linkCheckLimit, WriteConcern writeConcern);

    /**
     * Modifies an existing LinkCheckLimits record, if it doesn't exists then creates it.
     * @param linkCheckLimit modified or new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     */
    public void createOrModify(LinkCheckLimits linkCheckLimit, WriteConcern writeConcern);

    /**
     * Reads and returns a LinkCheckLimits object
     * @param id the unique id of the record
     * @return - found LinkCheckLimits object, it can be null
     */
    public LinkCheckLimits read(String id);

    /**
     * Updates a LinkCheckLimits record
     * @param linkCheckLimit the modified LinkCheckLimits object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean update(LinkCheckLimits linkCheckLimit, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     * @param linkCheckLimit the unnecessary object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean delete(LinkCheckLimits linkCheckLimit, WriteConcern writeConcern);

}
