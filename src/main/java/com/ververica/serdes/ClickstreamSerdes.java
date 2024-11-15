package com.ververica.serdes;

import com.google.gson.Gson;
import com.ververica.models.ClickEvent;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class ClickstreamSerdes extends AbstractDeserializationSchema<ClickEvent> {
    private Gson gson;

    @Override
    public void open(InitializationContext context) throws Exception {
        gson = new Gson();
        super.open(context);
    }

    @Override
    public ClickEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), ClickEvent.class);
    }
}