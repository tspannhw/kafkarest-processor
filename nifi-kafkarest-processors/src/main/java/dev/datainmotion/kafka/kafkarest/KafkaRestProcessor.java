/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.datainmotion.kafka.kafkarest;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"kafka,rest,proxy,consumer,single"})
@CapabilityDescription("Kafka Rest Processor, one at a time")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "kafka_url", description = "URL")})
@WritesAttributes({@WritesAttribute(attribute = "offset", description = "offset")})
public class KafkaRestProcessor extends AbstractProcessor {

    private KafkaRestService kafkaRestService = null;

    private static String KAFKA_URL_NAME = "KAFKA_URL";
    private static String KAFKA_TOPIC_NAME = "KAFKA_TOPIC";
    private static String OFFSETRESET_NAME = "OFFSETRESET";
    private static String MAX_POLL_RECORD_NAME = "MAXPOLLRECORDS";
    private static String CLIENTID_NAME = "CLIENT_ID";
    private static String GROUPID_NAME = "GROUP_ID";
    private static String ENABLE_AUTO_COMMIT_NAME = "ENABLEDAUTOCOMMIT";
    private static String KEY_DESERIALIZER_NAME = "KEY_DESERIALIZER";
    private static String VALUE_DESERIALIZER_NAME = "VALUE_DESERIALIZER";
    private static String MAX_NO_MESSAGE_COUNT_NAME = "MAXNOMESSAGECOUNT";
    private static String COMMIT_INTERVAL_NAME = "COMMITINTERVAL";

    public static final PropertyDescriptor COMMIT_INTERVAL = new PropertyDescriptor
            .Builder().name(COMMIT_INTERVAL_NAME)
            .displayName("COMMIT_INTERVAL")
            .description("COMMIT_INTERVAL")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_NO_MESSAGE_COUNT = new PropertyDescriptor
            .Builder().name(MAX_NO_MESSAGE_COUNT_NAME)
            .displayName("MAX_NO_MESSAGE_COUNT_NAME")
            .description("MAX_NO_MESSAGE_COUNT_NAME")
            .required(false)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALUE_DESERIALIZER = new PropertyDescriptor
            .Builder().name(VALUE_DESERIALIZER_NAME)
            .displayName("VALUE_DESERIALIZER_NAME")
            .description("VALUE_DESERIALIZER_NAME")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_DESERIALIZER = new PropertyDescriptor
            .Builder().name(KEY_DESERIALIZER_NAME)
            .displayName("KEY_DESERIALIZER_NAME")
            .description("KEY_DESERIALIZER_NAME")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ENABLE_AUTO_COMMIT= new PropertyDescriptor
            .Builder().name(ENABLE_AUTO_COMMIT_NAME)
            .displayName("ENABLE_AUTO_COMMIT_NAME")
            .description("ENABLE_AUTO_COMMIT_NAME")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GROUPID = new PropertyDescriptor
            .Builder().name(GROUPID_NAME)
            .displayName("GROUPID_NAME")
            .description("GROUPID_NAME")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENTID = new PropertyDescriptor
            .Builder().name(CLIENTID_NAME)
            .displayName("CLIENTID_NAME")
            .description("CLIENTID_NAME")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_POLL_RECORD = new PropertyDescriptor
            .Builder().name(MAX_POLL_RECORD_NAME)
            .displayName("Max poll Record")
            .description("Max Poll Record")
            .required(false)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .build();

    public static final PropertyDescriptor OFFSETRESET = new PropertyDescriptor
            .Builder().name(OFFSETRESET_NAME)
            .displayName("Offset reset")
            .description("Offset reset")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KAFKA_URL = new PropertyDescriptor
            .Builder().name(KAFKA_URL_NAME)
            .displayName("Kafka URL")
            .description("Kafka URL")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KAFKA_TOPIC = new PropertyDescriptor
            .Builder().name(KAFKA_TOPIC_NAME)
            .displayName("Kafka Topic")
            .description("Kafka Topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully determined image.").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to determine image.").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(KAFKA_URL);
        descriptors.add(KAFKA_TOPIC);
        descriptors.add(OFFSETRESET);
        descriptors.add(MAX_POLL_RECORD);
        descriptors.add(CLIENTID);
        descriptors.add(GROUPID);
        descriptors.add(ENABLE_AUTO_COMMIT);
        descriptors.add(KEY_DESERIALIZER);
        descriptors.add(VALUE_DESERIALIZER);
        descriptors.add(MAX_NO_MESSAGE_COUNT);
        descriptors.add(COMMIT_INTERVAL);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        kafkaRestService = new KafkaRestService();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (session == null || context == null) {
            return;
        }
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }
        try {
            flowFile.getAttributes();

            if (kafkaRestService == null) {
                kafkaRestService = new KafkaRestService();
            }
            String kafkaURL = flowFile.getAttribute(KAFKA_URL_NAME);
            if (kafkaURL == null) {
                kafkaURL = context.getProperty(KAFKA_URL_NAME).evaluateAttributeExpressions(flowFile).getValue();
            }
            if (kafkaURL == null) {
                kafkaURL = "localhost:2181";
            }
            final String kafkaStringURL = kafkaURL;

            String kafkaTopic = flowFile.getAttribute(KAFKA_TOPIC_NAME);
            if (kafkaTopic == null) {
                kafkaTopic = context.getProperty(KAFKA_TOPIC_NAME).evaluateAttributeExpressions(flowFile).getValue();
            }
            if (kafkaTopic == null) {
                kafkaTopic = "iot";
            }

            final String kafkaTopicFinal = kafkaTopic;
            final HashMap<String, String> attributes = new HashMap<String, String>();

            KafkaOptions options = new KafkaOptions();
            options.setTopic( kafkaTopic );
            options.setBroker( kafkaURL );
            options.setClientId( context.getProperty(CLIENTID).evaluateAttributeExpressions(flowFile).getValue() );
            options.setGroupId( context.getProperty(GROUPID).evaluateAttributeExpressions(flowFile).getValue() );
            options.setCommitInterval( context.getProperty(COMMIT_INTERVAL).evaluateAttributeExpressions(flowFile).getValue() );
            options.setEnableAutoCommit( context.getProperty(ENABLE_AUTO_COMMIT).evaluateAttributeExpressions(flowFile).getValue() );
            options.setKeyDeserializer( context.getProperty(KEY_DESERIALIZER).evaluateAttributeExpressions(flowFile).getValue() );
            options.setMaxNoMessageCount( context.getProperty(MAX_NO_MESSAGE_COUNT).evaluateAttributeExpressions(flowFile).getValue() );
            options.setMaxPollRecords( context.getProperty(MAX_POLL_RECORD).evaluateAttributeExpressions(flowFile).getValue() );
            options.setOffsetReset( context.getProperty(OFFSETRESET).evaluateAttributeExpressions(flowFile).getValue() );
            options.setValueDeserializer( context.getProperty(VALUE_DESERIALIZER).evaluateAttributeExpressions(flowFile).getValue() );
            attributes.put("kafka_topic", kafkaTopicFinal);
            attributes.put("kafka_url", kafkaStringURL);

            List<KafkaResult> results = kafkaRestService.consumeMessage( options );

            // TODO:  Do we return multiple records?   Do we send them to flowfile?

            try {
                if (results != null && results.size() > 0) {
                    for (KafkaResult result : results) {
                        for (KafkaHeader header : result.getKafkaHeaders()) {
                            attributes.put(header.getKey(), header.getValue());
                        }

//                        for (KafkaMetric kafkametric : result.getKafkaMetrics()) {
//                            attributes.put(kafkametric.getMetricName(), kafkametric.getMetricValue());
//                        }

                        attributes.put("kafka.message", result.getValue());
                        attributes.put("kafka.clientid", result.getClientId());
                        attributes.put("kafka.groupid", result.getGroupId());
                        attributes.put("kafka.key", result.getKey());
                        attributes.put("kafka.leader.epoch", result.getLeaderEpoch());
                        attributes.put("kafka.status.message", result.getStatusMessage());
                        attributes.put("kafka.timestamp.type", result.getTimestampType());
                        attributes.put("kafka.timestamp", String.valueOf(result.getTimestamp()));
                        attributes.put("kafka.offset", String.valueOf(result.getOffset()));
                        attributes.put("kafka.partition", String.valueOf(result.getPartition()));
                        attributes.put("kafka.record.count", String.valueOf(result.getRecordsFound()));
                    }
                }

               //  maybe set flow file to multiple records
            } catch (Exception x) {
                x.printStackTrace();
                attributes.put("kafka.error.message", x.getLocalizedMessage());
            }
            if (results != null) {
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                attributes.put("kafka.message.success", "false");
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_FAILURE);
            }

            session.commit();
        } catch (final Throwable t) {
            getLogger().error("Unable to read Kafka Message: " + t.getLocalizedMessage());
            throw new ProcessException(t);
        }
    }
}