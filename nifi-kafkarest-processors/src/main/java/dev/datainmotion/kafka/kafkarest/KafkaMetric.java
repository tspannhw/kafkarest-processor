package dev.datainmotion.kafka.kafkarest;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 *
 */
public class KafkaMetric {

    private List<KafkaMetricTag> kafkaMetricTags;

    private String clientId;
    private String description;
    private String group;
    private String metricName;
    private String metricValue;

    public KafkaMetric() {
        super();
    }

    public KafkaMetric(List<KafkaMetricTag> kafkaMetricTags, String clientId, String description, String group, String metricName, String metricValue) {
        super();
        this.kafkaMetricTags = kafkaMetricTags;
        this.clientId = clientId;
        this.description = description;
        this.group = group;
        this.metricName = metricName;
        this.metricValue = metricValue;
    }

    @Override
    public String toString() {
        return new StringJoiner( ", ", KafkaMetric.class.getSimpleName() + "[", "]" )
                .add( "kafkaMetricTags=" + kafkaMetricTags )
                .add( "clientId='" + clientId + "'" )
                .add( "description='" + description + "'" )
                .add( "group='" + group + "'" )
                .add( "metricName='" + metricName + "'" )
                .add( "metricValue='" + metricValue + "'" )
                .toString();
    }

    /**
     *
     * @param tag
     */
    public void addMetricTag(KafkaMetricTag tag) {
        if (kafkaMetricTags == null) {
            kafkaMetricTags = new ArrayList<>();
        }
        if (tag != null) {
            kafkaMetricTags.add(tag);
        }
    }
    public List<KafkaMetricTag> getKafkaMetricTags() {
        return kafkaMetricTags;
    }

    public void setKafkaMetricTags(List<KafkaMetricTag> kafkaMetricTags) {
        this.kafkaMetricTags = kafkaMetricTags;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getMetricValue() {
        return metricValue;
    }

    public void setMetricValue(String metricValue) {
        this.metricValue = metricValue;
    }

    /**
     *
     * //                    System.out.println( "Metric descr: " + clientid.description() );
     * //                    System.out.println( "Metric group: " + clientid.group() );
     * //                    System.out.println( "Metric name: " + metricMap.metricName() );
     * //                    System.out.println( "Metric value: " + metricMap.metricValue() );
     *  System.out.println( "------------------------For client id : " + clientid );
     * //
     * //                    if ( clientid != null && clientid.tags() != null) {
     * //                        for (Map.Entry<String, String> entry : clientid.tags().entrySet()) {
     * //                            String name = entry.getKey();
     * //                            String key = entry.getValue();
     * //                            System.out.println( "key=" + key + " value=" + name );
     */
}
