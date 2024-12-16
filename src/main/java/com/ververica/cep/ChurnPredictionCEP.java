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
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
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
import java.util.Objects;
import java.util.Properties;

import static com.ververica.config.AppConfig.buildSecurityProps;
import static com.ververica.utils.StreamingUtils.createClickEventConsumer;
import static com.ververica.utils.StreamingUtils.createKafkaAlertSink;

public class ChurnPredictionCEP {
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

        Pattern<ClickEvent, ?> churnPredictionPattern = Pattern.<ClickEvent>begin("first_view")
                .where(new SimpleCondition<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent event) {
                        return event.getEventType().equals("view");
                    }
                }).timesOrMore(10)  // User viewed multiple products
                .within(Time.days(7))
                .notNext("purchase");  // No purchase event occurs

        PatternStream<ClickEvent> churnPredictionStream = CEP.pattern(events.keyBy(ClickEvent::getUserId), churnPredictionPattern);

        SingleOutputStreamOperator<Alert> churnPredictionAlerts = churnPredictionStream.select(new PatternSelectFunction<ClickEvent, Alert>() {
            @Override
            public Alert select(Map<String, List<ClickEvent>> pattern) {
                ClickEvent firstView = pattern.get("first_view").get(0);
                String message = "Churn risk detected for user " + firstView.getUserId() +
                        ": viewed multiple products over the week without making a purchase.";
                return new Alert(firstView.getUserSession(), firstView.getUserId(), AlertType.CHURN_RISK, message);
            }
        }).name("ChurnPredictionAlertsPattern").uid("ChurnPredictionAlertsPattern");


        KafkaSink<Alert> kafkaAlertSink = createKafkaAlertSink(properties);
        churnPredictionAlerts.sinkTo(kafkaAlertSink);

        // Execute the pipeline
        environment.execute("E-commerce CEP Patterns Alerting");
    }
}