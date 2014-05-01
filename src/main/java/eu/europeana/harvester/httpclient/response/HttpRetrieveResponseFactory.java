package eu.europeana.harvester.httpclient.response;


/**
 * Factory that creates {@link eu.europeana.harvester.httpclient.response.HttpRetrieveResponse} instances based on type.
 */
public class HttpRetrieveResponseFactory {

    /**
     * @param type     - you have to choose the type of the response you need
     * @param basePath - if you choose disk based response you have to provide a base path for it
     * @return
     * @throws Exception
     */
    public HttpRetrieveResponse create(ResponseType type, String basePath) throws Exception {
        switch (type) {
            case MEMORY_STORAGE:
                return new HttpRetrieveResponseMemoryStorage();
            case DISK_STORAGE:
                return new HttpRetrieveReponseDiskStorage(basePath);
            default:
                throw new Exception("Type error");
        }
    }
}
