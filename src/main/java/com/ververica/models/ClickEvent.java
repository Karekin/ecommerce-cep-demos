package com.ververica.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClickEvent {
    private long eventTime;
    private String eventType;
    private String productId;
    private String catagoryId;
    private String categoryCode;
    private String brand;
    private double price;
    private String userId;
    private String userSession;
}