package eu.europeana.harvester.db.s3;

import com.typesafe.config.Config;

/**
 * Created by ymamakis on 12/9/16.
 */
public class BluemixConfiguration {

    private String region;
    private String bucket;
    private String clientKey;
    private String secretKey;
    private String endpoint;

    private BluemixConfiguration(final Config config){
        if (!config.hasPath("client_key")) throw new IllegalArgumentException("The configuration is missing the client_key property");
        clientKey = config.getString("client_key");
        if (!config.hasPath("secret_key")) throw new IllegalArgumentException("The configuration is missing the secret_key property");
        secretKey = config.getString("secret_key");
        if (!config.hasPath("region")) throw new IllegalArgumentException("The configuration is missing the region property");
        region = config.getString("region");
        if (!config.hasPath("bucket")) throw new IllegalArgumentException("The configuration is missing the bucket property");
        bucket = config.getString("bucket");
        if (!config.hasPath("endpoint")) throw new IllegalArgumentException("The configuration is missing the endpoint property");
        endpoint = config.getString("endpoint");
    }
    public static BluemixConfiguration valueOf(final Config config){
        return  new BluemixConfiguration(config);
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

    public String getEndpoint() {
        return endpoint;
    }
}
