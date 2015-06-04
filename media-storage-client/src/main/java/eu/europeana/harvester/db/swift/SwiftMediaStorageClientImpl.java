package eu.europeana.harvester.db.swift;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;
import com.sun.istack.internal.NotNull;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.MetaDataFields;
import org.apache.commons.io.IOUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.io.ContentMetadataBuilder;
import org.jclouds.io.MutableContentMetadata;
import org.jclouds.io.Payload;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.domain.Container;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.jclouds.openstack.swift.v1.options.PutOptions;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.jclouds.io.Payloads.newByteArrayPayload;

/**
 * Created by salexandru on 03.06.2015.
 */
public class SwiftMediaStorageClientImpl implements MediaStorageClient {
    private final ObjectApi objectApi;

    public SwiftMediaStorageClientImpl(@NotNull SwiftConfiguration config) {
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

        System.out.println ("withContent: " + withContent);


        final Map<String, String> swiftMetadata = swiftObject.getMetadata();

        System.out.println(swiftObject.getHeaders().toString());


        final String source = swiftMetadata.get(MetaDataFields.SOURCE.name());
        final List<String> aliases = decodeAliasesFromMetadata(swiftMetadata);
        final String originalUrl = swiftMetadata.get(MetaDataFields.ORIGINAL_URL.name());
        final Map<String, String> metaData = decodeExtraMetadata(swiftMetadata);
        final byte[] content = withContent ? IOUtils.toByteArray(swiftObject.getPayload().openStream()) : new byte[0];
        final String contentMd5 = computeMd5(content);

        System.out.println (source);
        System.out.println (swiftObject.getPayload().getContentMetadata().toString());

        System.out.println (IOUtils.toByteArray(swiftObject.getPayload().openStream()));
        System.out.println (content);

        DateTime createdAt = null;

        try {
            createdAt = DateTime.parse(swiftMetadata.get(MetaDataFields.CREATEDAT.name()));
        }
        catch (NullPointerException e) {

        }

        Integer versionNumber = null;

        try {
           versionNumber = Integer.getInteger(swiftMetadata.get(MetaDataFields.VERSIONNUMBER.name()));
        }
        catch(NullPointerException e) {

        }

        final String swiftObjectMd5 = swiftObject.getPayload().getContentMetadata().getContentMD5AsHashCode().toString();
        if (withContent && !contentMd5.equals(swiftObjectMd5)) {
            /*
             *  something wrong has happened to the data;
             *  security breach ?
             */
            System.out.println(contentMd5 + " " + swiftMetadata.get(MetaDataFields.MD5.name()));
            System.out.println(swiftObject.getPayload().getContentMetadata().getContentMD5AsHashCode().toString());
            throw new SecurityException("MD5 of content differs from expected");
        }

        return new MediaFile(source,
                             swiftObject.getName(),
                             aliases,
                             contentMd5,
                             originalUrl,
                             createdAt,
                             content,
                             versionNumber,
                             swiftObject.getPayload().getContentMetadata().getContentType(),
                             metaData,
                             swiftObject.getPayload().getContentMetadata().getContentLength().intValue()
                           );
    }

    @Override
    public void createOrModify (MediaFile mediaFile) {

        delete(mediaFile.getId());

        Map<String, String> metaData = new HashMap<>();
        metaData.put(MetaDataFields.SOURCE.name(), mediaFile.getSource());
        metaData.put(MetaDataFields.ORIGINAL_URL.name(), mediaFile.getOriginalUrl());
        metaData.put(MetaDataFields.VERSIONNUMBER.name(), Integer.toString(mediaFile.getVersionNumber()));
        metaData.put(MetaDataFields.CREATEDAT.name(), mediaFile.getCreatedAt().toString());
        metaData.put(MetaDataFields.MD5.name(), computeMd5(mediaFile.getContent()));

        System.out.println(computeMd5(mediaFile.getContent()));

        metaData.putAll(encodeExtraMetadata(mediaFile.getMetaData()));
        metaData.putAll(encodeAliases(mediaFile.getAliases()));


        final Payload payload = newByteArrayPayload(mediaFile.getContent());

        payload.getContentMetadata().setContentType(mediaFile.getContentType());
        payload.getContentMetadata().setContentLength(mediaFile.getLength());

        metaData = Collections.unmodifiableMap(metaData);
        objectApi.put(mediaFile.getId(), payload, PutOptions.Builder.metadata(metaData));
        objectApi.updateMetadata(mediaFile.getId(), metaData);

    }

    @Override
    public void delete (String id) {
        objectApi.delete(id);
    }

    private List<String> decodeAliasesFromMetadata(final Map<String, String> metadata) {
        final List<String> aliases = new ArrayList<>();
        final List<String> keys = new ArrayList<>();
        for (final Map.Entry<String, String> data: metadata.entrySet()) {
            if (data.getKey().startsWith(MetaDataFields.ALIASES.name())) {
                aliases.add(data.getValue());
                keys.add(data.getKey());
            }
        }

        return aliases;
    }

    private Map<String, String> decodeExtraMetadata(final Map<String, String> metadata) {
        final Map<String, String> extraMetadata = new HashMap<>();

        for (final Map.Entry<String, String> data: metadata.entrySet()) {
            if (data.getKey().startsWith(MetaDataFields.TECHNICAL_METADATA.name())) {
                extraMetadata.put(data.getKey().replaceFirst(MetaDataFields.TECHNICAL_METADATA.name(), ""),
                                  data.getValue());
            }
        }

        return extraMetadata;
    }

    private Map<String, String> encodeAliases(final List<String> aliases) {
        if (null == aliases || aliases.isEmpty()) {
            return Collections.EMPTY_MAP;
        }

        final Map<String, String> encodedAliases = new HashMap<>();

        for (final String alias: aliases) {
            encodedAliases.put(MetaDataFields.ALIASES.name() + alias, alias);
        }
        return encodedAliases;
    }

    private Map<String, String> encodeExtraMetadata(final Map<String, String> metadata) {
        if (null == metadata || metadata.isEmpty()) {
            return Collections.EMPTY_MAP;
        }

        final Map<String, String> encodedExtraMetadata = new HashMap<>();

        for (final Map.Entry<String, String> data: metadata.entrySet()) {
            encodedExtraMetadata.put(MetaDataFields.TECHNICAL_METADATA.name() + data.getKey(), data.getValue());
        }

        return encodedExtraMetadata;
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
