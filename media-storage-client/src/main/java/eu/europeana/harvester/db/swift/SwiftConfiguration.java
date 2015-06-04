package eu.europeana.harvester.db.swift;

/**
 * Created by salexandru on 03.06.2015.
 */
public class SwiftConfiguration {
    private final String authUrl;
    private final String userName;
    private final String password;
    private final String containerName;
    private final String regionName;
    private final String tenantName;

    public SwiftConfiguration (String authUrl, String tenantName, String userName, String password, String containerName,
                               String regionName)  {
        this.authUrl = authUrl;
        this.userName = userName;
        this.password = password;
        this.containerName = containerName;
        this.regionName = regionName;
        this.tenantName = tenantName;
    }

    public String getAuthUrl () {
        return authUrl;
    }

    public String getUserName () {
        return userName;
    }

    public String getPassword () {
        return password;
    }

    public String getContainerName () {
        return containerName;
    }

    public String getRegionName () {
        return regionName;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getIdentity() {
        return tenantName + ":" + userName;
    }
}
