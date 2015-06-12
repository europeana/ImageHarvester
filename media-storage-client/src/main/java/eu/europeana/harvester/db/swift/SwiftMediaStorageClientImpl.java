package eu.europeana.harvester.db.swift;

import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;
import org.apache.commons.io.IOUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.io.Payload;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.jclouds.openstack.swift.v1.options.PutOptions;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.jclouds.io.Payloads.newByteArrayPayload;


/**
 * Created by salexandru on 03.06.2015.
 */
public class SwiftMediaStorageClientImpl implements MediaStorageClient {
    private final ObjectApi objectApi;

    public SwiftMediaStorageClientImpl(SwiftConfiguration config) {
        if (null == config) {
            throw new IllegalArgumentException("Config cannot be null");
        }

        final SwiftApi swiftApi = ContextBuilder.newBuilder("openstack-swift")
                                 .credentials(config.getIdentity(), config.getPassword())
                                 .endpoint(config.getAuthUrl())
                                 .buildApi(SwiftApi.class);

        objectApi = swiftApi.getObjectApi(config.getRegionName(), config.getContainerName());

    }


    @Override
    public Boolean checkIfExists (String id) {
        return null != objectApi.getWithoutBody(id);
    }

    @Override
    public MediaFile retrieve (String id, Boolean withContent) throws IOException {

        final SwiftObject swiftObject = withContent ? objectApi.get(id) : objectApi.getWithoutBody(id);

        if (null == swiftObject) {
            return null;
        }

        final byte[] content = withContent ? IOUtils.toByteArray(swiftObject.getPayload().openStream()) : new byte[0];
        final String contentMd5 = computeMd5(content);


        final String swiftObjectMd5 = swiftObject.getPayload().getContentMetadata().getContentMD5AsHashCode().toString();
        if (withContent && !contentMd5.equals(swiftObjectMd5)) {
            /*
             *  something wrong has happened to the data;
             *  security breach ?
             */
            throw new SecurityException("MD5 of content differs from expected");
        }

        return new MediaFile(id,
                             null,
                             swiftObject.getName(),
                             null,
                             contentMd5,
                             null,
                             null,
                             content,
                             null,
                             swiftObject.getMetadata().get("content-type"),
                             null,
                             Integer.parseInt(swiftObject.getMetadata().get("size"))
                           );
    }

    @Override
    public void createOrModify (MediaFile mediaFile) {
        delete(mediaFile.getId());

        final Payload payload = newByteArrayPayload(mediaFile.getContent());

        payload.getContentMetadata().setContentType(mediaFile.getContentType());
        payload.getContentMetadata().setContentLength(mediaFile.getLength());

        Map<String, String> metadata = new HashMap<>();

        metadata.put("Content-Type", mediaFile.getContentType());
        metadata.put("size", Integer.toString(mediaFile.getSize()));
        metadata = Collections.unmodifiableMap(metadata);

        objectApi.put(mediaFile.getId(), payload, PutOptions.Builder.metadata(metadata));
    }

    @Override
    public void delete (String id) {
        objectApi.delete(id);
    }

    private String computeMd5(final byte[] content) {
        try {
            final byte[] array = MessageDigest.getInstance("MD5").digest(content);
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
            }
            return sb.toString();
        }
        catch (NoSuchAlgorithmException e ) {
            return null;
        }
    }
}
