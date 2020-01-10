package dev.datainmotion.kafka.kafkarest;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 *
 */
public class KafkaResult {


    private String statusMessage = null;

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public KafkaResult() {
        super();
        init();
    }

    private void init(){
        if ( kafkaHeaders == null) {
            kafkaHeaders = new ArrayList();
        }
        if ( kafkaMetrics == null ) {
            kafkaMetrics = new ArrayList();
        }
    }

    public void addKafkaHeader(KafkaHeader header) {
        if (kafkaHeaders == null) {
            init();
        }
        if ( header != null) {
            kafkaHeaders.add(header);
        }
    }

    public void addKafkaMetric(KafkaMetric metric) {
        if (kafkaMetrics == null) {
            init();
        }
        if ( metric != null) {
            kafkaMetrics.add(metric);
        }
    }

    private int recordsFound = 0;

    /**
     *
     * @param recordsFound
     * @param kafkaHeaders
     * @param kafkaMetrics
     * @param key
     * @param value
     * @param partition
     * @param offset
     * @param timestamp
     * @param leaderEpoch
     * @param clientId
     * @param groupId
     */
    public KafkaResult(int recordsFound, List<KafkaHeader> kafkaHeaders, List<KafkaMetric> kafkaMetrics, String key,
                       String value, int partition, long offset, long timestamp,
                       String leaderEpoch, String clientId, String groupId) {
        super();
        init();
        this.recordsFound = recordsFound;
        this.kafkaHeaders = kafkaHeaders;
        this.kafkaMetrics = kafkaMetrics;
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.leaderEpoch = leaderEpoch;
        this.clientId = clientId;
        this.groupId = groupId;
    }

    @Override
    public String toString() {
        return new StringJoiner( ", ", KafkaResult.class.getSimpleName() + "[", "]" )
                .add( "recordsFound=" + recordsFound )
                .add( "kafkaHeaders=" + kafkaHeaders )
                .add( "kafkaMetrics=" + kafkaMetrics )
                .add( "key='" + key + "'" )
                .add( "value='" + value + "'" )
                .add( "partition='" + partition + "'" )
                .add( "offset='" + offset + "'" )
                .add( "timestamp='" + timestamp + "'" )
                .add( "leaderEpoch='" + leaderEpoch + "'" )
                .add( "clientId='" + clientId + "'" )
                .add( "groupId='" + groupId + "'" )
                .toString();
    }

    public int getRecordsFound() {
        return recordsFound;
    }

    public void setRecordsFound(int recordsFound) {
        this.recordsFound = recordsFound;
    }

    private List<KafkaHeader> kafkaHeaders = null;

    private List<KafkaMetric> kafkaMetrics = null;

    private String key = null;

    private String value = null;

    private int partition = -1;

    private long offset = -1L;

    private long timestamp = -1L;

    private String leaderEpoch = null;

    private String clientId = null;

    private String groupId = null;

    public KafkaResult(List<KafkaHeader> kafkaHeaders, List<KafkaMetric> kafkaMetrics, String key,
                       String value, int partition, long offset, long timestamp, String leaderEpoch,
                       String clientId, String groupId) {
        super();
        init();
        this.kafkaHeaders = kafkaHeaders;
        this.kafkaMetrics = kafkaMetrics;
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.leaderEpoch = leaderEpoch;
        this.clientId = clientId;
        this.groupId = groupId;
    }


    public void setKafkaHeaders(List<KafkaHeader> kafkaHeaders) {
        this.kafkaHeaders = kafkaHeaders;
    }

    public void setKafkaMetrics(List<KafkaMetric> kafkaMetrics) {
        this.kafkaMetrics = kafkaMetrics;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setLeaderEpoch(String leaderEpoch) {
        this.leaderEpoch = leaderEpoch;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public List<KafkaHeader> getKafkaHeaders() {
        return kafkaHeaders;
    }

    public List<KafkaMetric> getKafkaMetrics() {
        return kafkaMetrics;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getLeaderEpoch() {
        return leaderEpoch;
    }

    public String getClientId() {
        return clientId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setRecordCount(int recordsFound) {
        setRecordsFound( recordsFound );
    }

    private String timestampType = null;

    public String getTimestampType() {
        return timestampType;
    }

    public void setTimestampType(String name) {
        timestampType = name;
    }
}
