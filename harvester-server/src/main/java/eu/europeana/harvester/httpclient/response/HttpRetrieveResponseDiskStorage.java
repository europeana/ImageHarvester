package eu.europeana.harvester.httpclient.response;

import com.google.common.io.Files;
import org.apache.commons.lang3.StringUtils;
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
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Path to file is blank");
        }
        this.absolutePath = path;
    }

    @Override
    synchronized public void init() throws IOException {
        contentSizeInBytes = 0l;
        try {
            final File file = new File(absolutePath);


            if(file.exists()) {
                if (!file.delete()) {
                    throw new RuntimeException("couldn't delete file " + absolutePath + " for unknown reason");
                }
            }

            if (!file.createNewFile()) {
                throw new RuntimeException("createNewFile: " + absolutePath + " has failed for unknown reason");
            }

            fo = new FileOutputStream(file.getAbsoluteFile());
        } catch (IOException | RuntimeException  e) {
            setState(RetrievingState.ERROR);
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
    synchronized public byte[] getContent() throws IOException {
        final File file = new File(absolutePath);
        final byte[] data = Files.toByteArray(file);
        return data;
    }

    @Override
    synchronized public void addContent(byte[] content) throws Exception {
        //lazy load
        if (null == fo) init();
        try {
            contentSizeInBytes += content.length;
            fo.write(content);
            fo.flush();
        } catch (IOException e) {
            setState(RetrievingState.ERROR);
            setException(e);

            close();

            throw e;
        }
    }

    @Override
    synchronized public Long getContentSizeInBytes() {
        return contentSizeInBytes;
    }

    @Override
    synchronized public void close() throws IOException {
       if (null != fo) fo.close();
    }

    @Override
    protected void finalize() throws Throwable {
        if (null != fo && fo.getChannel().isOpen()) {
            LOG.error ("File: " + absolutePath + " has channel still opened");
            try {
                fo.close();
            }
            catch (Exception e) {

            }
        }
        super.finalize();
    }
}
