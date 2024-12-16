package com.ververica.cep;

import com.ververica.models.Alert;
import com.ververica.models.AlertType;
import com.ververica.models.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
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

public class PurchaseIntentScoringCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = buildSecurityProps(new Properties());

        KafkaSource<ClickEvent> clickstreamKafkaSource = createClickEventConsumer(properties);

        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime());

        DataStream<ClickEvent> events = environment
                .fromSource(clickstreamKafkaSource, watermarkStrategy, "Clickstream Events Source")
                .name("ClickstreamEventsSource");

        Pattern<ClickEvent, ?> purchaseIntentPattern = Pattern.<ClickEvent>begin("initial_view")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("view");
                    }
                })
                .followedBy("repeat_view")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("view");
                    }
                }).times(3).within(Time.minutes(15));

        PatternStream<ClickEvent> purchaseIntentStream = CEP.pattern(events.keyBy(ClickEvent::getUserSession), purchaseIntentPattern);

        SingleOutputStreamOperator<Alert> purchaseIntentAlerts = purchaseIntentStream.select(new PatternSelectFunction<ClickEvent, Alert>() {
            @Override
            public Alert select(Map<String, List<ClickEvent>> pattern) {
                ClickEvent initialView = pattern.get("initial_view").get(0);
                String message = "High purchase intent detected for user " + initialView.getUserId() +
                        " on product " + initialView.getProductId();

                return new Alert(initialView.getUserSession(), initialView.getUserId(), AlertType.PRICE_SENSITIVITY, message);
            }
        }).name("PurchaseIntentAlertsPattern").uid("PurchaseIntentAlertsPattern");


        KafkaSink<Alert> kafkaAlertSink = createKafkaAlertSink(properties);
        purchaseIntentAlerts.sinkTo(kafkaAlertSink);

        environment.execute("E-commerce CEP Patterns Alerting");
    }
}