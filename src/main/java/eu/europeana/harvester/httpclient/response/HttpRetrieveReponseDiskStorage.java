package eu.europeana.harvester.httpclient.response;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Stores the retrieved content on disk thus minimizing the memory usage to hold only meta info (ie. headers, url, etc.)
 */
public class HttpRetrieveReponseDiskStorage extends HttpRetrieveResponseBase implements HttpRetriveResponse {

    /**
     * Used to compute MD5 on url's that are later used to generate unique file names for disk storage.
     */
    private static final HashFunction hf = Hashing.md5();

    /**
     * The file where to store the content.
     */
    private final FileOutputStream fo;

    /**
     * The base base path where the files are stored.
     */
    private final String basePath;

    private final String absolutePath;

    public HttpRetrieveReponseDiskStorage(String basePath) throws IOException {
        this.basePath = basePath;

        try {
            final HashCode hc = hf.newHasher()
                    .putString(getUrl().getPath(), Charsets.UTF_8)
                    .hash();
            absolutePath = (basePath + "/" + hc.toString());
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

    synchronized public String getBasePath() {
        return basePath;
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
