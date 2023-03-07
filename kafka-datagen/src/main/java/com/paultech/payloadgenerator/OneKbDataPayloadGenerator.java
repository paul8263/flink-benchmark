package com.paultech.payloadgenerator;

public class OneKbDataPayloadGenerator implements PayloadGenerator {

    public static final String ONE_KB_STRING = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Molestie at elementum eu facilisis sed odio morbi quis. Purus semper eget duis at tellus. Magna sit amet purus gravida quis blandit turpis cursus in. Diam in arcu cursus euismod quis viverra. Venenatis urna cursus eget nunc scelerisque viverra mauris in. Purus gravida quis blandit turpis cursus. Consectetur adipiscing elit duis tristique. Duis at consectetur lorem donec massa sapien faucibus. Eu turpis egestas pretium aenean pharetra magna ac placerat. Sit amet consectetur adipiscing elit duis tristique sollicitudin nibh sit. Egestas sed tempus urna et pharetra pharetra massa massa. Gravida dictum fusce ut placerat orci nulla pellentesque dignissim enim. Viverra mauris in aliquam sem fringilla ut. Risus nec feugiat in fermentum posuere urna nec. Urna duis convallis convallis tellus id interdum velit laoreet. Amet consectetur adipiscing elit duis tristique sollicitudin. Sollicitudin nibh ";


    @Override
    public String generatePayload() {
        return ONE_KB_STRING;
    }

    @Override
    public long getPayloadSize() {
        return 1024;
    }
}
