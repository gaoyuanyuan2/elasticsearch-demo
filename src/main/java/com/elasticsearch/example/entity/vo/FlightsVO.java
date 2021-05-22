package com.elasticsearch.example.entity.vo;

import lombok.Data;

import java.util.Map;

@Data
public class FlightsVO {

    private Price price;
    private Map map;

    @Data
    public static class Price {
        private long count;
        private double max;
        private double min;
        private double avg;
        private double total;
    }
}
