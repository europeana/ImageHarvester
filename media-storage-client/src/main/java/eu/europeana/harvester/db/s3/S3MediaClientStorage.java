package eu.europeana.harvester.db.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by ymamakis on 12/8/16.
 */
public class S3MediaClientStorage implements MediaStorageClient {


    private AmazonS3 client;
    private String bucket;

    public S3MediaClientStorage(String clientKey, String secretKey, String region, String bucket){
        AWSCredentials credentials = new BasicAWSCredentials(clientKey,secretKey);
        client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(region).build();
        this.bucket = bucket;
    }

    public S3MediaClientStorage(S3Configuration configuration){
        AWSCredentials credentials = new BasicAWSCredentials(configuration.getClientKey(),configuration.getSecretKey());
        client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(configuration.getRegion()).build();
        this.bucket = configuration.getBucket();

    }

    @Override
    public Boolean checkIfExists(String id) {

        return client.doesObjectExist(bucket,id);
    }

    @Override
    public MediaFile retrieve(String id, Boolean withContent) throws IOException, NoSuchAlgorithmException {

        S3Object object = client.getObject(bucket,id);
        if(object!=null) {
            final byte[] content = withContent ? IOUtils.toByteArray(object.getObjectContent()) : new byte[0];
            final String contentMd5 = object.getObjectMetadata().getContentMD5();
            if (withContent && !contentMd5.equals(computeMd5(content))) {
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
        client.deleteObject(bucket,mediaFile.getId());
      //  S3Object object = new S3Object();
       // object.setBucketName(bucket);
       // object.setKey(mediaFile.getId());
        ByteArrayInputStream stream = new ByteArrayInputStream(mediaFile.getContent());
       // object.setObjectContent(stream);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(mediaFile.getContentType());
        metadata.setContentLength(mediaFile.getSize());
        metadata.setContentMD5(mediaFile.getContentMd5());
        client.putObject(new PutObjectRequest(bucket,mediaFile.getId(),stream, metadata));

    }


    @Override
    public void delete(String id) throws IOException {
        client.deleteObject(bucket,id);
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
