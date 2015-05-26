package eu.europeana.crfmigration.domain;

import eu.europeana.harvester.domain.ReferenceOwner;

import java.util.List;

public class EuropeanaEDMObject {
    final ReferenceOwner referenceOwner;
    private final String edmObject;
    private final String edmIsShownBy;
    private final String edmIsShownAt;
    private final List<String> edmHasViews;

    public EuropeanaEDMObject(ReferenceOwner referenceOwner,String edmObject, String edmIsShownBy, String edmIsShownAt, List<String> edmHasViews) {
        this.referenceOwner = referenceOwner;
        this.edmObject = edmObject;
        this.edmIsShownBy = edmIsShownBy;
        this.edmIsShownAt = edmIsShownAt;
        this.edmHasViews = edmHasViews;
    }

    public ReferenceOwner getReferenceOwner() {
        return referenceOwner;
    }

    public String getEdmObject() {
        return edmObject;
    }

    public String getEdmIsShownBy() {
        return edmIsShownBy;
    }

    public String getEdmIsShownAt() {
        return edmIsShownAt;
    }

    public List<String> getEdmHasViews() {
        return edmHasViews;
    }
}
