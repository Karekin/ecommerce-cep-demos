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
import java.util.Objects;
import java.util.Properties;

import static com.ververica.config.AppConfig.buildSecurityProps;
import static com.ververica.utils.StreamingUtils.createClickEventConsumer;
import static com.ververica.utils.StreamingUtils.createKafkaAlertSink;

public class CrossSellOpCEP {
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

        Pattern<ClickEvent, ?> crossSellPattern = Pattern.<ClickEvent>begin("first_view")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("view");
                    }
                })
                .next("second_view")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("view");
                    }
                }).within(Time.minutes(5));

        PatternStream<ClickEvent> crossSellPatternStream = CEP.pattern(
                events.keyBy(ClickEvent::getUserSession),
                crossSellPattern
        );

        SingleOutputStreamOperator<Alert> crossSellAlerts = crossSellPatternStream.select(new PatternSelectFunction<ClickEvent, Alert>() {
            @Override
            public Alert select(Map<String, List<ClickEvent>> pattern) {
                ClickEvent firstView = pattern.get("first_view").get(0);
                ClickEvent secondView = pattern.get("second_view").get(0);

                // Check if the categories are different
                if (!firstView.getCategoryCode().equals(secondView.getCategoryCode())) {
                    var message = "Cross-sell opportunity detected for user " + firstView.getUserId() +
                            ": viewed products in categories " + firstView.getCategoryCode() + " and " + secondView.getCategoryCode();

                    return new Alert(secondView.getUserSession(), secondView.getUserId(), AlertType.CROSS_UPSELL, message);
                }
                return null; // Return null if categories are the same, effectively filtering it out
            }
        }).filter(Objects::nonNull).name("CrossSellAlertsPattern").uid("CrossSellAlertsPattern");


        KafkaSink<Alert> kafkaAlertSink = createKafkaAlertSink(properties);
        crossSellAlerts.sinkTo(kafkaAlertSink);

        environment.execute("E-commerce CEP Patterns Alerting");
    }
}