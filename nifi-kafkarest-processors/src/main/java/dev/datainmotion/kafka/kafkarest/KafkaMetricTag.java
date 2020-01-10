package dev.datainmotion.kafka.kafkarest;

import java.util.StringJoiner;

public class KafkaMetricTag {


    private String key;
    private String value;

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
        return new StringJoiner( ", ", KafkaMetricTag.class.getSimpleName() + "[", "]" )
                .add( "key='" + key + "'" )
                .add( "value='" + value + "'" )
                .toString();
    }

    public KafkaMetricTag(String key, String value) {
        super();
        this.key = key;
        this.value = value;
    }

    public KafkaMetricTag() {
        super();
    }
}
