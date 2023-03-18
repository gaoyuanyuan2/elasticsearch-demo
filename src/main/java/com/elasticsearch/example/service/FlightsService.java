package com.elasticsearch.example.service;

import com.elasticsearch.example.entity.vo.FlightsVO;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.FLIGHTS_INDEX;

@Service
public class FlightsService {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 求最大值
     *
     * @return
     * @throws IOException
     */
    public Map statMax() throws IOException {
        SearchRequest searchRequest = new SearchRequest(FLIGHTS_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        MinAggregationBuilder minAggregationBuilder =
                AggregationBuilders.min("minPrice").field("AvgTicketPrice");
        MaxAggregationBuilder maxAggregationBuilder =
                AggregationBuilders.max("maxPrice").field("AvgTicketPrice");
        SumAggregationBuilder sumAggregationBuilder =
                AggregationBuilders.sum("totalPrice").field("AvgTicketPrice");
        AvgAggregationBuilder avgAggregationBuilder =
                AggregationBuilders.avg("avgPrice").field("AvgTicketPrice");
        searchSourceBuilder.aggregation(minAggregationBuilder);
        searchSourceBuilder.aggregation(maxAggregationBuilder);
        searchSourceBuilder.aggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(avgAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        Max maxPrice = searchResponse.getAggregations().get("maxPrice");
        Min minPrice = searchResponse.getAggregations().get("minPrice");
        Avg avgPrice = searchResponse.getAggregations().get("avgPrice");
        Sum totalPrice = searchResponse.getAggregations().get("totalPrice");
        System.out.println("maxPrice: " + maxPrice.getValue());
        System.out.println("minPrice: " + minPrice.getValue());
        System.out.println("avgPrice: " + avgPrice.getValue());
        System.out.println("totalPrice: " + totalPrice.getValue());
        Map<String, Double> resp = new HashMap<>();
        resp.put("maxPrice", maxPrice.getValue());
        resp.put("minPrice", minPrice.getValue());
        resp.put("avgPrice", avgPrice.getValue());
        resp.put("totalPrice", totalPrice.getValue());
        return resp;
    }

    /**
     * 按目的地分组 统计价格信息+天气信息
     *
     * @throws IOException
     */
    public Map termDestCountry() throws IOException {
        SearchRequest searchRequest = new SearchRequest(FLIGHTS_INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);

        TermsAggregationBuilder destCountryAggregationBuilder =
                AggregationBuilders.terms("destCountryTerms").field(
                        "DestCountry");
        StatsAggregationBuilder statsAggregationBuilder =
                AggregationBuilders.stats("statsPrice").field("AvgTicketPrice");
        TermsAggregationBuilder destWeatherAggregationBuilder =
                AggregationBuilders.terms("destWeather").field("DestWeather");
        destCountryAggregationBuilder.subAggregation(statsAggregationBuilder);
        destCountryAggregationBuilder.subAggregation(destWeatherAggregationBuilder);
        searchSourceBuilder.aggregation(destCountryAggregationBuilder);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        Terms destCountryTerms = searchResponse.getAggregations().get(
                "destCountryTerms");
        List<? extends Terms.Bucket> buckets = destCountryTerms.getBuckets();
        Map<String, FlightsVO> resp = new HashMap<>();
        for (Terms.Bucket bucket : buckets) {
            System.out.println("key:" + bucket.getKeyAsString() +
                    "\ndoc_count:" + bucket.getDocCount());

            FlightsVO vo = new FlightsVO();
            Stats statsPrice = bucket.getAggregations().get("statsPrice");
            System.out.println("count: " + statsPrice.getCount());
            System.out.println("max: " + statsPrice.getMax());
            System.out.println("min: " + statsPrice.getMin());
            System.out.println("avg: " + statsPrice.getAvg());
            System.out.println("total: " + statsPrice.getSum());
            FlightsVO.Price flightsPrice = new FlightsVO.Price();
            flightsPrice.setCount(statsPrice.getCount());
            flightsPrice.setMax(statsPrice.getMax());
            flightsPrice.setMin(statsPrice.getMin());
            flightsPrice.setAvg(statsPrice.getAvg());
            flightsPrice.setTotal(statsPrice.getSum());
            vo.setPrice(flightsPrice);

            ParsedStringTerms terms = bucket.getAggregations().get(
                    "destWeather");
            Map<String, Object> weatherMap = new HashMap<>();
            List<? extends Terms.Bucket> destWeatherTerms = terms.getBuckets();
            for (Terms.Bucket weatherTerms : destWeatherTerms) {
                weatherMap.put(weatherTerms.getKeyAsString(),
                        weatherTerms.getDocCount());
            }
            vo.setMap(weatherMap);
            resp.put(bucket.getKeyAsString(), vo);
        }
        return resp;
    }

    /**
     * 分组统计 group by buyer,supplier,orderType
     *
     * @return 分组条件组装
     */
    public TermsAggregationBuilder getOrderCountAggregationBuilder() {
        TermsAggregationBuilder orderTypeGroup = AggregationBuilders.terms("orderType")
                .field("orderType");
        TermsAggregationBuilder supplierGroup = AggregationBuilders.terms("supplier")
                .field("supplier").subAggregation(orderTypeGroup);
        TermsAggregationBuilder buyerGroup = AggregationBuilders.terms("buyer")
                .field("buyer").subAggregation(supplierGroup);
        return buyerGroup;
    }
}
