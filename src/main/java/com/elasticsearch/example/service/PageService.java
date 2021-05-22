package com.elasticsearch.example.service;

import com.elasticsearch.example.util.ESUtils;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.TMDB_INDEX;
import static com.elasticsearch.example.util.Constant.USER_INDEX;

@Service
public class PageService {

    @Autowired
    private RestHighLevelClient client;

    /**
     * @return
     * @throws IOException
     */
    public List<Map> fromSize() throws IOException {
        SearchRequest searchRequest = new SearchRequest(TMDB_INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.sort("id", SortOrder.DESC);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(response);
    }


    public List<Map> searchAfter() throws IOException {
        SearchRequest searchRequest = new SearchRequest(USER_INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.sort("age", SortOrder.DESC).sort("_id",
                SortOrder.ASC);
        searchSourceBuilder.size(1);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Map> map = new ArrayList<>();
        while (hits != null && hits.length > 0) {
            Object[] sortValues = null;
            for (SearchHit hit : hits) {
                System.out.println(hit.getSourceAsString());
                map.add(hit.getSourceAsMap());
                sortValues = hit.getSortValues();
            }

            //第二步: 下一页
            searchSourceBuilder.searchAfter(sortValues);
            searchRequest.source(searchSourceBuilder);
            searchResponse = client.search(searchRequest,
                    RequestOptions.DEFAULT);
            hits = searchResponse.getHits().getHits();
        }
        return map;
    }


    public List<Map> scroll() throws IOException {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

        SearchRequest searchRequest = new SearchRequest(USER_INDEX);
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(1);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest,
                RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Map> map = new ArrayList<>();
        while (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                map.add(hit.getSourceAsMap());
                System.out.println(hit.getSourceAsString());
            }

            //第二步: 下一页
            SearchScrollRequest searchScrollRequest =
                    new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(scroll);
            searchResponse = client.scroll(searchScrollRequest,
                    RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            hits = searchResponse.getHits().getHits();
        }

        //清除scroll快照
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        return map;
    }

}
