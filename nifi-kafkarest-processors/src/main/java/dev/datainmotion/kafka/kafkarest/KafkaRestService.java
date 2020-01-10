package dev.datainmotion.kafka.kafkarest;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 *
 */
public class KafkaRestService {

    public static final String INVALID_KAFKA_URL_OR_TOPIC_NAME = "Invalid Kafka URL or Topic Name";
    public static final String INVALID_KAFKA_URL = "Invalid Kafka URL";

    public KafkaRestService() {
        super();
    }

    // TODO:  reuse kafka stuff
    // https://github.com/apache/nifi/blob/d570fe81543b706acf9a797fb05fac68a384b3f4/nifi-nar-bundles/nifi-kafka-bundle/nifi-kafka-2-0-processors/src/main/java/org/apache/nifi/processors/kafka/pubsub/ConsumeKafkaRecord_2_0.java
    // https://github.com/apache/nifi/blob/0e1a37fcb9bc6fcc5c801a34d19673745c908437/nifi-nar-bundles/nifi-kafka-bundle/nifi-kafka-0-9-processors/src/main/java/org/apache/nifi/processors/kafka/pubsub/KafkaProcessorUtils.java

    // https://kafka.apache.org/22/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
    public KafkaResult sendMessage(KafkaMessage message) {

        return null;
    }

    /**
     * https://stackoverflow.com/questions/31412294/java-check-not-null-empty-else-assign-default-value
     * @param value a String to check for null and replace with empty ""
     * @return String of valid value of empty string, props doesn't like null
     */
    public static String orElse(String value) {
        String defaultValue = "";
        return Optional.ofNullable(value).orElse(defaultValue);
    }

    /**
     *
     */
    public List<KafkaResult> consumeMessage(KafkaOptions options) {

        if (options == null || options.getBroker() == null | options.getTopic() == null ) {
            return null;
        }

        // should validate broker that it has a port
        List<KafkaResult> kafkaResults = new ArrayList<>(  );
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,options.getBroker());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, orElse(options.getGroupId()));
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,orElse(options.getClientId()));
        props.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, orElse(options.getKeyDeserializer()));
        props.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, orElse(options.getValueDeserializer()));
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, orElse(options.getMaxPollRecords()));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, orElse(options.getEnableAutoCommit()));
//        props.setProperty("auto.commit.interval.ms", options.);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, orElse(options.getOffsetReset())); //earlier, latest
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(options.getTopic()));

        int totalRecordCountToReturn = 1;
        int recordsFound = 0;

        try {
            int noMessageFound = 0;
            while (true) {

                KafkaResult result = new KafkaResult(  );
                result.setClientId( options.getClientId() );
                result.setGroupId( options.getGroupId() );

//                // produces a lot of metrics, might want a processor just for this
//                Map<MetricName, ? extends Metric> metrics = consumer.metrics();
//                for (Map.Entry<MetricName, ? extends Metric> e : metrics.entrySet()) {
//                    MetricName metricNameKey = e.getKey();
//                    Metric metricMap = e.getValue();
//                   // System.out.println( "------------------------For client id : " + clientid );
//                    KafkaMetric kafkaMetric = new KafkaMetric();
//
//                    if ( metricNameKey != null && metricNameKey.tags() != null) {
//                        for (Map.Entry<String, String> entry : metricNameKey.tags().entrySet()) {
//                            String key = entry.getKey();
//                            String value = entry.getValue();
//                            kafkaMetric.addMetricTag( new KafkaMetricTag( key, value ) );
//                        }
//                    }
//
//                    kafkaMetric.setClientId( clientId );
//                    kafkaMetric.setDescription( metricNameKey.description() );
//                    kafkaMetric.setGroup( metricNameKey.group() );
//                    kafkaMetric.setMetricName( metricMap.metricName().name() );
//                    kafkaMetric.setMetricValue( metricMap.metricValue().toString() );
//                    result.addKafkaMetric( kafkaMetric );
//                }

                // get to an offset consumer.seek(  );
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                int maxNoMessageFoundCount = 10;

                if ( options.getMaxNoMessageCount() != null ) {
                    try {
                        maxNoMessageFoundCount = Integer.parseInt(options.getMaxNoMessageCount());
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        maxNoMessageFoundCount = 10;
                    }
                }
                // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
                if (consumerRecords.count() == 0) {
                    noMessageFound++;
                    if (noMessageFound >  maxNoMessageFoundCount)
                        // If no message found count is reached to threshold exit loop.
                        break;
                    else
                        continue;
                }
                recordsFound++;
                result.setRecordCount(recordsFound);
                if ( recordsFound > totalRecordCountToReturn) {
                    break;
                }
                //System.out.println("Consumer Records Count: " + consumerRecords.count());
                //print each record.
                consumerRecords.forEach(record -> {
                    result.setKey(record.key());
                    result.setValue(record.value());
                    result.setPartition( record.partition() );
                    result.setOffset( record.offset() );
                    result.setTimestamp( record.timestamp() );

                    if ( record.leaderEpoch() != null ) {
                        result.setLeaderEpoch( record.leaderEpoch().get().toString() );
                    }
                    result.setTimestampType( record.timestampType().name );

                    if (record.headers() != null ) {
                        record.headers().forEach(header -> {
                            if ( header.key() != null && header.value() != null ) {
                                result.addKafkaHeader( new KafkaHeader( header.key(), String.valueOf(header.value()) ) );
                            }
                        });
                    }
                });

                kafkaResults.add(result);
                // commits the offset of record to broker.
                consumer.commitAsync();
            }
            consumer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return kafkaResults;
    }
}