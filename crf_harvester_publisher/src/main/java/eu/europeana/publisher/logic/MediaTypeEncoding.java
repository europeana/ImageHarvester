package eu.europeana.publisher.logic;

public enum MediaTypeEncoding {
    IMAGE(1),
    SOUND(2),
    VIDEO(3),
    TEXT(4);

    private final int value;

    private MediaTypeEncoding(final int value) {
        this.value = value;
    }

    public int getValue()          {return value;}
    public int getEncodedValue()   {return value << TagEncoding.MEDIA_TYPE.getBitPos();}
}
