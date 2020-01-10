package dev.datainmotion.kafka.kafkarest;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.StringJoiner;

public class KafkaOptions {

    public KafkaOptions() {
        super();
    }

    private String broker;
    private String topic;
    private String offsetReset;
    private String maxPollRecords;
    private String clientId;
    private String groupId;
    private String enableAutoCommit;
    private String keyDeserializer;
    private String valueDeserializer;
    private String maxNoMessageCount;
    private String commitInterval;

    public KafkaOptions(String broker, String topic, String offsetReset, String maxPollRecords, String clientId, String groupId, String enableAutoCommit, String keyDeserializer, String valueDeserializer, String maxNoMessageCount, String commitInterval) {
        super();
        this.broker = broker;
        this.topic = topic;
        this.offsetReset = offsetReset;
        this.maxPollRecords = maxPollRecords;
        this.clientId = clientId;
        this.groupId = groupId;
        this.enableAutoCommit = enableAutoCommit;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.maxNoMessageCount = maxNoMessageCount;
        this.commitInterval = commitInterval;
    }

    @Override
    public String toString() {
        return new StringJoiner( ", ", KafkaOptions.class.getSimpleName() + "[", "]" )
                .add( "broker='" + broker + "'" )
                .add( "topic='" + topic + "'" )
                .add( "offsetReset='" + offsetReset + "'" )
                .add( "maxPollRecords='" + maxPollRecords + "'" )
                .add( "clientId='" + clientId + "'" )
                .add( "groupId='" + groupId + "'" )
                .add( "enableAutoCommit='" + enableAutoCommit + "'" )
                .add( "keyDeserializer='" + keyDeserializer + "'" )
                .add( "valueDeserializer='" + valueDeserializer + "'" )
                .add( "maxNoMessageCount='" + maxNoMessageCount + "'" )
                .add( "commitInterval='" + commitInterval + "'" )
                .toString();
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getOffsetReset() {
        return offsetReset;
    }

    public void setOffsetReset(String offsetReset) {
        this.offsetReset = offsetReset;
    }

    public String getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(String maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(String enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    public String getMaxNoMessageCount() {
        return maxNoMessageCount;
    }

    public void setMaxNoMessageCount(String maxNoMessageCount) {
        this.maxNoMessageCount = maxNoMessageCount;
    }

    public String getCommitInterval() {
        return commitInterval;
    }

    public void setCommitInterval(String commitInterval) {
        this.commitInterval = commitInterval;
    }
}
