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

    public final static String idFromUrl(final String url,final String recordId) {
        final HashCode hc = hf.newHasher()
                .putString(new StringBuilder().append(url).append("-").append(recordId), Charsets.UTF_8)
                .hash();
        return hc.toString();
    }

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

    private final Boolean active;

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
        this.active = null;
    }

    public SourceDocumentReference(final ReferenceOwner referenceOwner,
                                   final String url, final String ipAddress, final String lastStatsId,
                                   final Long redirectPathDepth, final List<String> redirectionPath,
                                   final Boolean active) {
        this.id = SourceDocumentReference.idFromUrl(url,referenceOwner.getRecordId());
        this.referenceOwner = referenceOwner;
        this.url = url;
        this.ipAddress = ipAddress;
        this.lastStatsId = lastStatsId;
        this.redirectPathDepth = redirectPathDepth;
        this.redirectionPath = redirectionPath;
        this.active = active;
    }

    public SourceDocumentReference(final String id, final ReferenceOwner referenceOwner,
                                   final String url, final String ipAddress,
                                   final String lastStatsId, final Long redirectPathDepth,
                                   final List<String> redirectionPath, final Boolean active) {
        this.id = id;
        this.referenceOwner = referenceOwner;
        this.url = url;
        this.ipAddress = ipAddress;
        this.lastStatsId = lastStatsId;
        this.redirectPathDepth = redirectPathDepth;
        this.redirectionPath = redirectionPath;
        this.active = active;
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

    public String getLastStatsId() {
        return lastStatsId;
    }

    public Boolean getActive() {return active;}


    public SourceDocumentReference withLastStatsId(String newLastStatsId) {
        return new SourceDocumentReference(this.id, this.referenceOwner, this.url,
                this.ipAddress, newLastStatsId, this.redirectPathDepth, this.redirectionPath, this.active);
    }

    public SourceDocumentReference withRedirectionPath(List<String> newRedirectionPath) {
        return new SourceDocumentReference(this.id, this.referenceOwner, this.url,
                this.ipAddress, this.lastStatsId, new Long((newRedirectionPath != null)?newRedirectionPath.size():0), newRedirectionPath, this.active);
    }

    public SourceDocumentReference withIPAddress(String newIpAddress) {
        return new SourceDocumentReference(this.id, this.referenceOwner, this.url,
                newIpAddress, this.lastStatsId, this.redirectPathDepth, this.redirectionPath, this.active);
    }

    public SourceDocumentReference withActive(Boolean newActive) {
        return new SourceDocumentReference(this.id, this.referenceOwner, this.url,
                this.ipAddress, this.lastStatsId, this.redirectPathDepth, this.redirectionPath, newActive);
    }

}
