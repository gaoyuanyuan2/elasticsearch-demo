package com.elasticsearch.example.service;

import com.elasticsearch.example.util.ESUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.BLOGS_INDEX;

@Service
public class DisMaxQueryService {

    @Autowired
    private RestHighLevelClient client;

    public List<Map> disMax(String title,String body,float tieBreaker) throws Exception {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.fetchSource(new String[]{"*"}, Strings.EMPTY_ARRAY);

        // 将其他匹配语句的评分与tie_breaker 相乘
        DisMaxQueryBuilder disMaxQueryBuilder = QueryBuilders.disMaxQuery().tieBreaker(tieBreaker);
        MatchQueryBuilder matchQueryBuilder1 = QueryBuilders.matchQuery
                ("title", title);
        MatchQueryBuilder matchQueryBuilder2 = QueryBuilders.matchQuery
                ("body", body);
        disMaxQueryBuilder.add(matchQueryBuilder1);
        disMaxQueryBuilder.add(matchQueryBuilder2);
        sourceBuilder.query(disMaxQueryBuilder);
        SearchRequest searchRequest = new SearchRequest(BLOGS_INDEX);
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(response);
    }
}
