package com.paultech.payloadgenerator;

import java.util.UUID;

public class UuidPayloadGenerator implements PayloadGenerator {
    @Override
    public String generatePayload() {
        return UUID.randomUUID().toString();
    }

    @Override
    public long getPayloadSize() {
        return 36;
    }
}
