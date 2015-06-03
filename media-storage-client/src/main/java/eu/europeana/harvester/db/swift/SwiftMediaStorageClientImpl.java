package eu.europeana.harvester.db.swift;

import com.google.common.collect.ImmutableSet;
import com.sun.istack.internal.NotNull;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.MetaDataFields;
import com.google.inject.Module;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.jclouds.ContextBuilder;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.joda.time.DateTime;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by salexandru on 03.06.2015.
 */
public class SwiftMediaStorageClientImpl implements MediaStorageClient {
    private final Container container;

    public SwiftMediaStorageClientImpl(@NotNull AccountConfig config, @NotNull String containerName) {
        container = new AccountFactory(config).createAccount().getContainer(containerName);
    }


    @Override
    public Boolean checkIfExists (String id) {
        return container.getObject(id).exists();
    }

    @Override
    public MediaFile retrieve (String id, Boolean withContent) throws IOException {
        final StoredObject object = container.getObject(id);

        if (!object.exists()) {
            return null;
        }

        final String source = (String)object.getMetadata(MetaDataFields.SOURCE.name());
        final List<String> aliases = (List<String>)object.getMetadata(MetaDataFields.ALIASES.name());
        final String originalUrl = (String)object.getMetadata(MetaDataFields.ORIGINAL_URL.name());
        final Integer versionNumber = (Integer)object.getMetadata(MetaDataFields.VERSIONNUMBER.name());
        final Map<String, String> metaData = (Map<String, String>)object.getMetadata(MetaDataFields.TECHNICAL_METADATA.name());
        final byte[] content = withContent ? object.downloadObject() : null;
        final String contentMd5 = withContent ? computeMd5(content) : null;
        final DateTime createdAt = DateTime.parse((String) object.getMetadata("createdAt"));

        return new MediaFile(source, object.getName(), aliases, contentMd5, originalUrl, createdAt, content,
                             versionNumber, object.getContentType(), metaData, (int) object.getContentLength()
                           );
    }

    @Override
    public void createOrModify (MediaFile mediaFile) {
        final StoredObject object = container.getObject(mediaFile.getId());

        if (object.exists()) {
            object.delete();
        }

        final Map<String, Object> metaData = new HashMap<>();
        metaData.put(MetaDataFields.SOURCE.name(), mediaFile.getSource());
        metaData.put(MetaDataFields.ORIGINAL_URL.name(), mediaFile.getOriginalUrl());
        metaData.put(MetaDataFields.VERSIONNUMBER.name(), mediaFile.getVersionNumber());
        metaData.put(MetaDataFields.TECHNICAL_METADATA.name(), mediaFile.getMetaData());
        metaData.put(MetaDataFields.ALIASES.name(), mediaFile.getAliases());
        metaData.put("createdAt", mediaFile.getCreatedAt().toString());

        object.setContentLength(mediaFile.getLength());
        object.setContentType(mediaFile.getContentType());
        object.setMetadata(metaData);
        object.uploadObject(mediaFile.getContent());
    }

    @Override
    public void delete (String id) {
        container.getObject(id).delete();
    }

    /**
     *
     */
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
