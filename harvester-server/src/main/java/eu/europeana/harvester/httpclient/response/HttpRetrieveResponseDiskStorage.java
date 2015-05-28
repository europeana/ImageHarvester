package eu.europeana.harvester.httpclient.response;

import com.google.common.io.Files;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Stores the retrieved content on disk thus minimizing the memory usage to hold only meta info (ie. headers, url, etc.)
 */
public class HttpRetrieveResponseDiskStorage extends HttpRetrieveResponseBase implements HttpRetrieveResponse {

    private static final Logger LOG = LogManager.getLogger(HttpRetrieveResponseDiskStorage.class.getName());

    /**
     * The file where to store the content.
     */
    private FileOutputStream fo;

    /**
     * The absolute path on disk where the content of the download will be saved.
     */
    private final String absolutePath;

    public HttpRetrieveResponseDiskStorage(String path) throws IOException {
        this.absolutePath = path;
        init();
    }

    @Override
    public void init() throws IOException {
        contentSizeInBytes = 0l;
        try {
            final File file = new File(absolutePath);
            if(file.exists()) {
                file.delete();
            }
            file.createNewFile();
            fo = new FileOutputStream(file.getAbsoluteFile());
        } catch (IOException e) {
            setState(ResponseState.ERROR);
            setException(e);
            LOG.error(e.getMessage());
            throw e;
        }
    }

    @Override
    synchronized public String getAbsolutePath() {
        return absolutePath;
    }

    @Override
    synchronized public byte[] getContent() {
        final File file = new File(absolutePath);
        try {
            final byte[] data = Files.toByteArray(file);

            return data;
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }

        return null;
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
