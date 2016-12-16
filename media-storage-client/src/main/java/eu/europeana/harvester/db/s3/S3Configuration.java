package eu.europeana.harvester.db.s3;

import com.typesafe.config.Config;

/**
 * Created by ymamakis on 12/9/16.
 */
public class S3Configuration {

    private String region;
    private String bucket;
    private String clientKey;
    private String secretKey;

    private S3Configuration(final Config config){
        if (!config.hasPath("client_key")) {
            throw new IllegalArgumentException("The configuration is missing the client_key property");
        }
        clientKey = config.getString("client_key");

        if (!config.hasPath("secret_key")) {
            throw new IllegalArgumentException("The configuration is missing the secret_key property");
        }
        secretKey = config.getString("secret_key");
        if (!config.hasPath("region")) {
            throw new IllegalArgumentException("The configuration is missing the region property");
        }
        region = config.getString("region");
        if (!config.hasPath("bucket")) {
            throw new IllegalArgumentException("The configuration is missing the client_key property");
        }
        bucket = config.getString("bucket");
    }
    public static S3Configuration valueOf(final Config config){
        return  new S3Configuration(config);
    }
    public String getRegion(){
        return region;
    }

    public String getBucket() {
        return bucket;
    }

    public String getClientKey() {
        return clientKey;
    }

    public String getSecretKey() {
        return secretKey;
    }
}
