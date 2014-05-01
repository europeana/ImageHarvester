package eu.europeana.harvester.httpclient.response;

public class HttpRetrieveResponseMemoryStorage extends HttpRetrieveResponseBase implements HttpRetrieveResponse {

    private byte[] content;

    @Override
    public byte[] getContent() {
        return content;
    }

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
            setState(ResponseState.ERROR);
            setException(e);
            throw e;
        }
    }

    @Override
    synchronized public Long getContentSizeInBytes() {
        return (long) content.length;
    }

}
