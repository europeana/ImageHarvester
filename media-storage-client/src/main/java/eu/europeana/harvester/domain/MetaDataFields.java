package eu.europeana.harvester.domain;

/**
 * The metadata fields used at saving a MediaFile.
 */
public enum MetaDataFields {
    SOURCE,
    ORIGINAL_URL,
    VERSIONNUMBER,
    TECHNICAL_METADATA,
    ALIASES,

    //used in swift media storage
    CREATEDAT,
    MD5,


    //TODO: why I need this?
    _transientFields
}
