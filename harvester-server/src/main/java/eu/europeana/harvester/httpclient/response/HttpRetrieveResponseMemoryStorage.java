package eu.europeana.harvester.httpclient.response;

/**
 * Stores the retrieved content in memory.
 */
public class HttpRetrieveResponseMemoryStorage extends HttpRetrieveResponseBase implements HttpRetrieveResponse {

    /**
     * The total content of the document in bytes.
     */
    private byte[] content;

    @Override
    public void init() {
        this.content = null;
    }

    @Override
    public String getAbsolutePath() {
        return "";
    }

    @Override
    public byte[] getContent() {
        return content;
    }

    /**
     * Saves the content in memory to an byte array
     * @param content arrived packages content
     * @throws Exception
     */
    synchronized public void addContent(byte[] content) throws Exception {
        try {
            if (this.content == null) {
                this.content = content;
                return;
            }

            final byte[] newContent = new byte[this.content.length + content.length];
            System.arraycopy(this.content, 0, newContent, 0, this.content.length);
            System.arraycopy(content, 0, newContent, this.content.length, content.length);

            this.content = newContent;
        } catch (Exception e) {
            setState(RetrievingState.ERROR);
            setException(e);
            throw e;
        }
    }

    @Override
    synchronized public Long getContentSizeInBytes() {
        return (long) content.length;
    }

}
