package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.List;

/**
 * Represents the reference of a source document.
 */
public class SourceDocumentReference {

    /**
     * The id of the link. Used for storage identity/uniqueness.
     */
    @Id
    @Property("id")
	private final String id;

    /**
     * An object which contains: provider id, collection id, record id
     */
    private final ReferenceOwner referenceOwner;

    /**
     * The url.
     */
	private final String url;

    /**
     * The IP address.
     */
    private final String ipAddress;

    /**
     * The unique id of the last saved object with statistics.(SourceDocumentProcessingStatistics)
     */
    private final String lastStatsId;

    /**
     * The number of redirect links.
     */
    private final Long redirectPathDepth;

    /**
     * List of redirect links.
     */
    private final List<String> redirectionPath;

    /**
     * Used to compute MD5 on url's that are later used to generate unique id's.
     */
    private static final HashFunction hf = Hashing.md5();

    public SourceDocumentReference() {
        this.lastStatsId = null;
        this.id = null;
        this.referenceOwner = null;
        this.url = null;
        this.ipAddress = null;
        this.redirectPathDepth = null;
        this.redirectionPath = null;
    }

    public SourceDocumentReference(final ReferenceOwner referenceOwner, final String url, final String ipAddress,
                                   final String lastStatsId, final Long redirectPathDepth,
                                   final List<String> redirectionPath) {
        final HashCode hc = hf.newHasher()
                .putString(url, Charsets.UTF_8)
                .hash();
        this.id = hc.toString();
        this.referenceOwner = referenceOwner;
        this.url = url;
        this.ipAddress = ipAddress;
        this.lastStatsId = lastStatsId;
        this.redirectPathDepth = redirectPathDepth;
        this.redirectionPath = redirectionPath;
    }

    public SourceDocumentReference(final String id, final ReferenceOwner referenceOwner, final String url,
                                   final String ipAddress, final String lastStatsId, final Long redirectPathDepth,
                                   final List<String> redirectionPath) {
        this.id = id;
        this.referenceOwner = referenceOwner;
        this.url = url;
        this.ipAddress = ipAddress;
        this.lastStatsId = lastStatsId;
        this.redirectPathDepth = redirectPathDepth;
        this.redirectionPath = redirectionPath;
    }

    public String getId() {
        return id;
    }

    public ReferenceOwner getReferenceOwner() {
        return referenceOwner;
    }

    public String getUrl() {
        return url;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getLastStatsId() {
        return lastStatsId;
    }

    public Long getRedirectPathDepth() {
        return redirectPathDepth;
    }

    public List<String> getRedirectionPath() {
        return redirectionPath;
    }

    public SourceDocumentReference withLastStatsId(String id) {
        return new SourceDocumentReference(this.id, this.referenceOwner, this.url,
                this.ipAddress, id, this.redirectPathDepth, this.redirectionPath);
    }

    public SourceDocumentReference withRedirectionPath(List<String> redirectionPath) {
        return new SourceDocumentReference(this.id, this.referenceOwner, this.url,
                this.ipAddress, this.lastStatsId, (long)redirectionPath.size(), redirectionPath);
    }

    public SourceDocumentReference withIPAddress(String ipAddress) {
        return new SourceDocumentReference(this.id, this.referenceOwner, this.url,
                ipAddress, this.lastStatsId, this.redirectPathDepth, this.redirectionPath);
    }

}
