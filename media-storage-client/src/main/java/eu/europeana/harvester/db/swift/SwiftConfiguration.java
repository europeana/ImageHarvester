package eu.europeana.harvester.db.swift;

/**
 * Created by salexandru on 03.06.2015.
 */
public class SwiftConfiguration {
    private final String provider;
    private final String authUrl;
    private final String identity;
    private final String credential;
    private final String collectionName;

    public SwiftConfiguration (String provider, String authUrl, String identity, String credential, String collectionName) {
        this.provider = provider;
        this.authUrl = authUrl;
        this.identity = identity;
        this.credential = credential;
        this.collectionName = collectionName;
    }

    public String getProvider () {
        return provider;
    }

    public String getAuthUrl () {
        return authUrl;
    }

    public String getIdentity () {
        return identity;
    }

    public String getCredential () {
        return credential;
    }

    public String getCollectionName () {
        return collectionName;
    }
}
