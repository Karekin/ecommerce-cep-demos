package com.ververica.producers;

import com.ververica.config.AppConfig;
import com.ververica.models.ClickEvent;
import com.ververica.utils.DataSourceUtils;
import com.ververica.utils.StreamingUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Stream;

import static com.ververica.utils.StreamingUtils.closeProducer;


public class ClickEventProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(ClickEventProducer.class);

    public static void main(String[] args) throws IOException {
        Stream<ClickEvent> events = DataSourceUtils
                .loadDataFile("/data/events.csv")
                .map(DataSourceUtils::toClickEvent);

        Properties properties = AppConfig.buildProducerProps();
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "64000");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        logger.info("Starting Kafka Producer  ...");

        KafkaProducer eventProducer = new KafkaProducer<String, ClickEvent>(properties);

        logger.info("Generating click events ...");

        int count = 0;
        for (Iterator<ClickEvent> it = events.iterator(); it.hasNext(); ) {
            ClickEvent clickEvent = it.next();

            StreamingUtils.handleMessage(eventProducer, AppConfig.CLICKEVENTS_TOPIC, clickEvent.getUserSession(), clickEvent);

            count += 1;
            if (count % 10000000 == 0) {
                logger.info("Total so far {}.", count);
            }
        }

        logger.info("Total click events sent '{}'.", count);

        logger.info("Closing Producers ...");
        closeProducer(eventProducer);
    }
}
