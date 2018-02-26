package eu.europeana.harvester.db.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.Md5Utils;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by ymamakis on 12/8/16.
 */
public class S3MediaClientStorage implements MediaStorageClient {


    private AmazonS3 client;
    private String bucket;
    private boolean isBluemix = false;

    public S3MediaClientStorage(String clientKey, String secretKey, String region, String bucket){
        AWSCredentials credentials = new BasicAWSCredentials(clientKey,secretKey);
        client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(region).build();
        this.bucket = bucket;
    }

    public S3MediaClientStorage(S3Configuration configuration){
        AWSCredentials credentials = new BasicAWSCredentials(configuration.getClientKey(),configuration.getSecretKey());
        client = AmazonS3ClientBuilder.standard().withCredentials(
                new AWSStaticCredentialsProvider(credentials)).withRegion(configuration.getRegion()).build();
        this.bucket = configuration.getBucket();
    }

    /**
     * Create a new S3 client and specify an endpoint (bluemix). Calling this constructor sets the boolean isBlueMix
     * to true; in the Objectstorage project, it is used to switch to the correct way of constructing the Object URI
     * when using resource path addressing (used by Bluemix) instead of virtual host addressing (default usage with
     * Amazon S3). Not sure if this is an issue here, will remove if it's not needed
     * @param configuration
     */
    public S3MediaClientStorage(BluemixConfiguration configuration) {
        System.setProperty("com.amazonaws.sdk.disableDNSBuckets", "True");
        client = new AmazonS3Client(new BasicAWSCredentials(configuration.getClientKey(), configuration.getSecretKey()));
        client.setS3ClientOptions(
                S3ClientOptions.builder()
                        .setPathStyleAccess(true)
                        .disableChunkedEncoding()
                        .build());
        client.setEndpoint(configuration.getEndpoint());
        this.bucket = configuration.getBucket();
        isBluemix = true;
    }

    @Override
    public Boolean checkIfExists(String id) {
        return client.doesObjectExist(bucket,id);
    }

    @Override
    public MediaFile retrieve(String id, Boolean withContent) throws IOException, NoSuchAlgorithmException {

        S3Object object = client.getObject(bucket,id);
        if(object != null) {
            final byte[] content = withContent ? IOUtils.toByteArray(object.getObjectContent()) : new byte[0];
            final String contentMd5 = isBluemix ? object.getObjectMetadata().getUserMetaDataOf("UserContentMD5")
            : object.getObjectMetadata().getETag();

            if (withContent && !contentMd5.equals(Md5Utils.md5AsBase64(content))) {
            /*
             *  something wrong has happened to the data;
             *  security breach ?
             */
                throw new SecurityException("MD5 of content differs from expected");
            }

            return new MediaFile(id,
                    null,
                    object.getKey(),
                    null,
                    contentMd5,
                    null,
                    null,
                    content,
                    null,
                    object.getObjectMetadata().getContentType(),
                    null,
                    Integer.parseInt(Long.toString(object.getObjectMetadata().getContentLength()))
            );
        }
        return null;
    }

    @Override
    public void createOrModify(MediaFile mediaFile) {
        client.deleteObject(bucket, mediaFile.getId());
        createOrModifyNoDel(mediaFile);
    }

    public void createOrModifyNoDel(MediaFile mediaFile) {
        ByteArrayInputStream stream = new ByteArrayInputStream(mediaFile.getContent());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(mediaFile.getContentType());
        metadata.setContentLength(mediaFile.getSize());
        if (isBluemix) {
            metadata.getUserMetadata().put("UserContentMD5", mediaFile.getContentMd5());
        } else {
            metadata.setContentMD5(mediaFile.getContentMd5());
        }
        client.putObject(new PutObjectRequest(bucket, mediaFile.getId(), stream, metadata));
    }

    @Override
    public void delete(String id) throws IOException {
        client.deleteObject(bucket, id);
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
