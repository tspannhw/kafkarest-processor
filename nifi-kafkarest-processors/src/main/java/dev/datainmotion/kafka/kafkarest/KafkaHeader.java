package dev.datainmotion.kafka.kafkarest;

import java.util.StringJoiner;

public class KafkaHeader {

    /**
     *        System.out.println( "Header Key " + header.key() + ", Header Value:  " + header.value() );
     */

    private String key;
    private String value;

    public KafkaHeader() {
        super();
    }

    public KafkaHeader(String key, String value) {
        super();
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return new StringJoiner( ", ", KafkaHeader.class.getSimpleName() + "[", "]" )
                .add( "key='" + key + "'" )
                .add( "value='" + value + "'" )
                .toString();
    }
}
