package com.ververica.cep;

import com.ververica.models.Alert;
import com.ververica.models.AlertType;
import com.ververica.models.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.ververica.config.AppConfig.buildSecurityProps;
import static com.ververica.utils.StreamingUtils.createClickEventConsumer;
import static com.ververica.utils.StreamingUtils.createKafkaAlertSink;

public class PriceSensitivityDetectionCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        var properties = buildSecurityProps(new Properties());

        KafkaSource<ClickEvent> clickstreamKafkaSource = createClickEventConsumer(properties);

        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime());

        DataStream<ClickEvent> events = environment
                .fromSource(clickstreamKafkaSource, watermarkStrategy, "Clickstream Events Source")
                .name("ClickstreamEventsSource");

        Pattern<ClickEvent, ?> priceSensitivityPattern = Pattern.<ClickEvent>begin("initial_view")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("view");
                    }
                })
                .next("view_price_drop")
                .where(new IterativeCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event, Context<ClickEvent> ctx) throws Exception {
                        ClickEvent initialView = ctx.getEventsForPattern("initial_view").iterator().next();
                        return event.getEventType().equals("view") && event.getProductId().equals(initialView.getProductId()) && event.getPrice() < initialView.getPrice();
                    }
                })
                .next("cart_after_price_drop")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("cart");
                    }
                })
                .within(Time.minutes(10));

        PatternStream<ClickEvent> priceSensitivityStream = CEP.pattern(events.keyBy(ClickEvent::getUserId), priceSensitivityPattern);

        SingleOutputStreamOperator<Alert> priceSensitivityAlerts = priceSensitivityStream.select(new PatternSelectFunction<ClickEvent, Alert>() {
            @Override
            public Alert select(Map<String, List<ClickEvent>> pattern) {
                ClickEvent initialView = pattern.get("initial_view").get(0);
                var message = "Price-sensitive customer detected for user " + initialView.getUserId() +
                        " on product " + initialView.getProductId() + " after a price drop.";
                return new Alert(initialView.getUserSession(), initialView.getUserId(), AlertType.PRICE_SENSITIVITY, message);
            }
        }).name("PriceSensitivityAlertsPattern").uid("PriceSensitivityAlertsPattern");


        KafkaSink<Alert> kafkaAlertSink = createKafkaAlertSink(properties);
        priceSensitivityAlerts.sinkTo(kafkaAlertSink);

        environment.execute("E-commerce CEP Patterns Alerting");
    }
}