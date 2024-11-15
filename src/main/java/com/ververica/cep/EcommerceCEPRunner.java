package com.ververica.cep;

import com.ververica.models.Alert;
import com.ververica.models.AlertType;
import com.ververica.models.ClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
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

public class EcommerceCEPRunner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        var properties = buildSecurityProps(new Properties());

        // 1. Create a Kafka/Flink Consumer
        KafkaSource<ClickEvent> clickstreamKafkaSource = createClickEventConsumer(properties);

        // 2. Create a Watermark Strategy
        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime());

        // 3. Create a DataStream from the Kafka Source
        DataStream<ClickEvent> events = environment
                .fromSource(clickstreamKafkaSource, watermarkStrategy, "Clickstream Events Source")
                .name("ClickstreamEventsSource")
                .uid("ClickstreamEventsSource");


        // 4. Define CEP Patterns
        // Pattern 1: Cross-Sell and Upsell Opportunities
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

        // Pattern 2: High-Value Cart Abandonment Detection with Timeout
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

                        var message = "High-value cart abandonment detected for user '" + cartAdd.getUserId() + "' priced at " + cartAdd.getPrice() +
                                ". No purchase within 30 minutes.";

                        return new Alert(cartAdd.getUserSession(), cartAdd.getUserId(), AlertType.CART_ABANDONMENT, message);
                    }
                }, new PatternSelectFunction<ClickEvent, Alert>() {
                    @Override
                    public Alert select(Map<String, List<ClickEvent>> pattern) {
                        ClickEvent cartAdd = pattern.get("cart_add").get(0);
                        ClickEvent purchase = pattern.get("purchase").get(0);

                        var message = "Purchase completed for user " + purchase.getUserId() + " on product " + purchase.getProductId() +
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


//        // Pattern 3: Purchase Intent Scoring Based on User Engagement
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
                var message = "High purchase intent detected for user " + initialView.getUserId() +
                        " on product " + initialView.getProductId();

                return new Alert(initialView.getUserSession(), initialView.getUserId(), AlertType.PRICE_SENSITIVITY, message);
            }
        }).name("PurchaseIntentAlertsPattern").uid("PurchaseIntentAlertsPattern");

        // Pattern 4: Price Sensitivity Detection
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


        // Pattern 5: Churn Prediction for Frequent Viewers Without Conversion
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
                var message = "Churn risk detected for user " + firstView.getUserId() +
                        ": viewed multiple products over the week without making a purchase.";
                return new Alert(firstView.getUserSession(), firstView.getUserId(), AlertType.CHURN_RISK, message);
            }
        }).name("ChurnPredictionAlertsPattern").uid("ChurnPredictionAlertsPattern");

        // 5. Union all the Streams
        var alertStream = crossSellAlerts
                .union(
                        abandonedmentAlert,
                        priceSensitivityAlerts,
                        purchaseIntentAlerts,
                        churnPredictionAlerts
                );

        // 6. Create a Kafka Sink and Sink the Alerts
        KafkaSink<Alert> kafkaAlertSink = createKafkaAlertSink(properties);
        alertStream.sinkTo(kafkaAlertSink);

        // 7. Execute the pipeline
        environment.execute("E-commerce CEP Patterns Alerting");
    }
}