package eu.europeana.harvester.db;

import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.domain.WebResourceMetaInfo;

import java.util.List;

/**
 * DAO for CRUD with WebResourceMetaInfoDAO collection
 */
public interface WebResourceMetaInfoDAO {


    /**
     * Persists a WebResourceMetaInfo object
     * @param webResourceMetaInfo - a new object
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return returns if the operation was successful
     */
    public boolean create(WebResourceMetaInfo webResourceMetaInfo, WriteConcern writeConcern);

    /**
     * Persists a WebResourceMetaInfo object
     * @param webResourceMetaInfos - a list of new objects
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     */
    public void create(List<WebResourceMetaInfo> webResourceMetaInfos, WriteConcern writeConcern);


    /**
     * Reads and returns a WebResourceMetaInfo object
     * @param id the unique id of the record
     * @return - found WebResourceMetaInfo object, it can be null
     */
    public WebResourceMetaInfo read(String id);

    /**
     * Reads and returns a list of WebResourceMetaInfo objects
     * @param ids the unique ids of the records
     * @return - found WebResourceMetaInfo objects
     */
    public List<WebResourceMetaInfo> read(List<String> ids);

    /**
     * Updates a WebResourceMetaInfo record
     * @param webResourceMetaInfo the modified WebResourceMetaInfo
     * @param writeConcern describes the guarantee that MongoDB provides when reporting on the success of a write
     *                     operation
     * @return - success or failure
     */
    public boolean update(WebResourceMetaInfo webResourceMetaInfo, WriteConcern writeConcern);

    /**
     * Deletes a record from DB
     * @param id the unique id of the record
     * @return - an object which contains all information about this operation
     */
    public WriteResult delete(String id);
}
