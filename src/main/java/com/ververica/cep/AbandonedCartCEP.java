package com.ververica.cep;

import com.ververica.config.AppConfig;
import com.ververica.models.Alert;
import com.ververica.models.AlertType;
import com.ververica.models.ClickEvent;
import com.ververica.serdes.AlertSerializer;
import com.ververica.serdes.ClickstreamSerdes;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Either;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.ververica.config.AppConfig.buildSecurityProps;
import static com.ververica.utils.StreamingUtils.createKafkaAlertSink;

public class AbandonedCartCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = buildSecurityProps(new Properties());

        // 1 Create Kafka/Flink Consumer
        KafkaSource<ClickEvent> clickstreamKafkaSource = KafkaSource.<ClickEvent>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                .setTopics(AppConfig.CLICKEVENTS_TOPIC)
                .setGroupId("clickstream.consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ClickstreamSerdes())
                .setProperties(properties)
                .build();

        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime());

        DataStream<ClickEvent> events = environment
                .fromSource(clickstreamKafkaSource, watermarkStrategy, "Clickstream Events Source")
                .name("ClickstreamEventsSource");

        Pattern<ClickEvent, ?> abandonmentPattern = Pattern.<ClickEvent>begin("cart_add")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("cart") && event.getPrice() > 200;
                    }
                })
                .followedBy("purchase")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("purchase");
                    }
                }).within(Time.minutes(30));

        PatternStream<ClickEvent> abandonmentPatternStream = CEP.pattern(
                events.keyBy(ClickEvent::getUserSession), abandonmentPattern
        );

        SingleOutputStreamOperator<Alert> abandonedmentAlert = abandonmentPatternStream
                .select(new PatternTimeoutFunction<ClickEvent, Alert>() {
                    @Override
                    public Alert timeout(Map<String, List<ClickEvent>> pattern, long timeoutTimestamp) {
                        ClickEvent cartAdd = pattern.get("cart_add").get(0);

                        String message = "High-value cart abandonment detected for user '" + cartAdd.getUserId() + "' priced at " + cartAdd.getPrice() +
                                ". No purchase within 30 minutes.";

                        return new Alert(cartAdd.getUserSession(), cartAdd.getUserId(), AlertType.CART_ABANDONMENT, message);
                    }
                }, new PatternSelectFunction<ClickEvent, Alert>() {
                    @Override
                    public Alert select(Map<String, List<ClickEvent>> pattern) {
                        ClickEvent cartAdd = pattern.get("cart_add").get(0);
                        ClickEvent purchase = pattern.get("purchase").get(0);

                        String message = "Purchase completed for user " + purchase.getUserId() + " on product " + purchase.getProductId() +
                                " priced at " + purchase.getPrice();

                        return new Alert(cartAdd.getUserSession(), cartAdd.getUserId(), AlertType.PURCHASE_COMPLETION, message);
                    }
                }).map(new MapFunction<Either<Alert, Alert>, Alert>() {
                    @Override
                    public Alert map(Either<Alert, Alert> alert) throws Exception {
                        if (alert.isLeft()) {
                            return alert.left();
                        } else {
                            return alert.right();
                        }
                    }
                }).name("AbandonmentAlertsPattern").uid("AbandonmentAlertsPattern");

        KafkaSink<Alert> kafkaAlertSink = createKafkaAlertSink(properties);
        abandonedmentAlert.sinkTo(kafkaAlertSink);

        // Execute the pipeline
        environment.execute("E-commerce CEP Patterns Alerting");
    }
}