package com.paultech.payloadgenerator;

import com.paultech.PayloadType;

public class PayloadGeneratorBuilder {
    public static PayloadGenerator build(PayloadType payloadType) {
        switch (payloadType) {
            case ONE_KB:
                return new OneKbDataPayloadGenerator();
            case UUID:
            default:
                return new UuidPayloadGenerator();
        }
    }
}
