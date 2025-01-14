package io.factorhouse.kpow.key;

/**
 * An internal class which represents a hierarchical identifier used by Kpow's {@link KeyStrategy} to key metrics data.
 */
public class Taxon {

    private final String domain;
    private final String domainId;
    private final String object;
    private final String objectId;

    /**
     * Constructs a new Taxon instance.
     * @param domain the domain of the taxon
     * @param domainId the domain ID of the taxon
     * @param object the object of the taxon
     * @param objectId the object ID of the taxon
     */
    public Taxon(
        String domain,
        String domainId,
        String object,
        String objectId
    ) {
        this.domainId = domainId;
        this.domain = domain;
        this.object = object;
        this.objectId = objectId;
    }

    /**
     * Returns the domain of the Taxon.
     *
     * @return the domain
     */
    public String getDomain() {
        return domain;
    }

    /**
     * Returns the object of the Taxon.
     *
     * @return the object
     */
    public String getObject() {
        return object;
    }

    /**
     * Returns the object ID of the Taxon.
     *
     * @return the object ID
     */
    public String getObjectId() {
        return objectId;
    }

    /**
     * Returns the domain ID of the Taxon.
     *
     * @return the domain ID
     */
    public String getDomainId() {
        return domainId;
    }
}
