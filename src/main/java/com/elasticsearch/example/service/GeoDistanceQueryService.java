package com.elasticsearch.example.service;

import com.elasticsearch.example.util.ESUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.BLOGS_INDEX;

@Service
public class GeoDistanceQueryService {
    @Autowired
    private RestHighLevelClient client;

    public List<Map> findPage(double latitude, double longitude, String distance) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.fetchSource(new String[]{"*"}, Strings.EMPTY_ARRAY);
        // 间接实现了QueryBuilder接口
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // 以某点为中心，搜索指定范围
        GeoDistanceQueryBuilder distanceQueryBuilder = new GeoDistanceQueryBuilder("location");
        distanceQueryBuilder.point(latitude, longitude);
        // 定义查询单位：公里
        distanceQueryBuilder.distance(distance, DistanceUnit.KILOMETERS);
        boolQueryBuilder.filter(distanceQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(BLOGS_INDEX);
        boolQueryBuilder.filter(distanceQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(response);
    }
}
