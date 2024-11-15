package com.ververica.utils;

import com.ververica.config.AppConfig;
import com.ververica.models.Alert;
import com.ververica.models.ClickEvent;
import com.ververica.serdes.AlertSerializer;
import com.ververica.serdes.ClickstreamSerdes;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamingUtils {
    private static final Logger logger
            = LoggerFactory.getLogger(StreamingUtils.class);

    public static <K,V> void handleMessage(KafkaProducer<K, V> producer, String topic, K key, V value) {
        var record = new ProducerRecord(topic, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception !=null) {
                logger.error("Error while producing: ", exception);
            } else {
//                logger.info("Successfully stored offset '{}': partition: {} - {}", metadata.offset(), metadata.partition(), metadata.topic());
            }
        });
    }

    public static <K, V> void closeProducer(KafkaProducer<K, V> producer) {
        producer.flush();
        producer.close();
    }

    public static KafkaSource<ClickEvent> createClickEventConsumer(Properties properties) {
        return KafkaSource.<ClickEvent>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                .setTopics(AppConfig.CLICKEVENTS_TOPIC)
                .setGroupId(AppConfig.CONSUMER_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ClickstreamSerdes())
                .setProperties(properties)
                .build();
    }

    public static KafkaSink<Alert> createKafkaAlertSink(Properties properties) {
        return KafkaSink.<Alert>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                .setRecordSerializer(new AlertSerializer(AppConfig.ALERTS_TOPIC))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(properties)
                .build();
    }
}