package eu.europeana.harvester.httpclient.response;

/**
 * Used for link check. Does not stores anything from the document.
 */
public class HttpRetrieveResponseWithNoStorage extends HttpRetrieveResponseBase implements HttpRetrieveResponse {

    @Override
    public void init() {

    }

    @Override
    public String getAbsolutePath() {
        return "";
    }

    @Override
    public byte[] getContent() {
        return null;
    }

    @Override
    public void addContent(byte[] content) throws Exception {
        // we don't store anything
    }
}
