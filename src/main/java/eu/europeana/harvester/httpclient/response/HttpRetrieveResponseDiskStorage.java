package eu.europeana.harvester.httpclient.response;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Stores the retrieved content on disk thus minimizing the memory usage to hold only meta info (ie. headers, url, etc.)
 */
public class HttpRetrieveResponseDiskStorage extends HttpRetrieveResponseBase implements HttpRetrieveResponse {

    /**
     * The file where to store the content.
     */
    private final FileOutputStream fo;

    /**
     * The absolute path on disk where the content of the download will be saved.
     */
    private final String absolutePath;

    public HttpRetrieveResponseDiskStorage(String path) throws IOException {
        this.absolutePath = path;
        try {
            final File file = new File(absolutePath);
            file.createNewFile();
            fo = new FileOutputStream(file.getAbsoluteFile());
        } catch (IOException e) {
            setState(ResponseState.ERROR);
            setException(e);
            throw e;
        }
    }

    synchronized public String getAbsolutePath() {
        return absolutePath;
    }

    @Override
    synchronized public byte[] getContent() {
        return new byte[]{};
    }

    @Override
    synchronized public void addContent(byte[] content) throws Exception {
        try {
            contentSizeInBytes += content.length;
            fo.write(content);
        } catch (IOException e) {
            setState(ResponseState.ERROR);
            setException(e);
            throw e;
        }
    }

    @Override
    synchronized public Long getContentSizeInBytes() {
        return contentSizeInBytes;
    }

    @Override
    synchronized public void close() throws IOException {
        fo.close();
    }

}
