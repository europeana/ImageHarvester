package eu.europeana.harvester.db.s3;

import com.typesafe.config.Config;

/**
 * Created by luthien on 31/1/18.
 */
public class BluemixConfiguration {

    private String bucket;
    private String clientKey;
    private String secretKey;
    private String endpoint;

    private BluemixConfiguration(final Config config){
        if (!config.hasPath("client_key")) throw new IllegalArgumentException("The configuration is missing the client_key property");
        clientKey = config.getString("client_key");
        if (!config.hasPath("secret_key")) throw new IllegalArgumentException("The configuration is missing the secret_key property");
        secretKey = config.getString("secret_key");
        if (!config.hasPath("bucket")) throw new IllegalArgumentException("The configuration is missing the bucket property");
        bucket = config.getString("bucket");
        if (!config.hasPath("endpoint")) throw new IllegalArgumentException("The configuration is missing the endpoint property");
        endpoint = config.getString("endpoint");
    }
    public static BluemixConfiguration valueOf(final Config config){
        return  new BluemixConfiguration(config);
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
