package eu.europeana.crfmigration.domain;

public class MongoConfig {

    /**
     * Source MongoDB host.
     */
    private final String sourceHost;

    /**
     * Source MongoDB port.
     */
    private final Integer sourcePort;

    /**
     * The source DB name where the thumbnails will be stored.
     */
    private final String sourceDBName;

    /**
     * The username of the source DB if it is encrypted
     */
    private final String sourceDBUsername;

    /**
     * The password of the source DB if it is encrypted
     */
    private final String sourceDBPassword;


    /**
     * Target MongoDB host.
     */
    private final String targetHost;

    /**
     * Target MongoDB port.
     */
    private final Integer targetPort;

    /**
     * The target DB name where the thumbnails will be stored.
     */
    private final String targetDBName;

    /**
     * The username of the target DB if it is encrypted
     */
    private final String targetDBUsername;

    /**
     * The password of the target DB if it is encrypted
     */
    private final String targetDBPassword;

    public MongoConfig(String sourceHost, Integer sourcePort, String sourceDBName,
                       String sourceDBUsername, String sourceDBPassword,
                       String targetHost, Integer targetPort, String targetDBName,
                       String targetDBUsername, String targetDBPassword) {
        this.sourceHost = sourceHost;
        this.sourcePort = sourcePort;
        this.sourceDBName = sourceDBName;
        this.sourceDBUsername = sourceDBUsername;
        this.sourceDBPassword = sourceDBPassword;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.targetDBName = targetDBName;
        this.targetDBUsername = targetDBUsername;
        this.targetDBPassword = targetDBPassword;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public Integer getSourcePort() {
        return sourcePort;
    }

    public String getSourceDBName() {
        return sourceDBName;
    }

    public String getTargetHost() {
        return targetHost;
    }

    public Integer getTargetPort() {
        return targetPort;
    }

    public String getTargetDBName() {
        return targetDBName;
    }

    public String getSourceDBUsername() {
        return sourceDBUsername;
    }

    public String getSourceDBPassword() {
        return sourceDBPassword;
    }

    public String getTargetDBUsername() {
        return targetDBUsername;
    }

    public String getTargetDBPassword() {
        return targetDBPassword;
    }
}
