package com.elasticsearch.example.service;

import com.elasticsearch.example.util.ESUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.ADDRESS_INDEX;
import static com.elasticsearch.example.util.Constant.TITLES_INDEX;

@Service
public class MultiMatchService {

    @Autowired
    private RestHighLevelClient client;

    public List<Map> mostFields() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.fetchSource(new String[]{"*"}, Strings.EMPTY_ARRAY);
        // 权重^10
        MultiMatchQueryBuilder multiMatchQueryBuilder =
                QueryBuilders.multiMatchQuery("barking dogs")
                        .field("title.std")
                        .field("title", 10)// 权重值
                        .type(MultiMatchQueryBuilder.Type.MOST_FIELDS);
        return getMaps(sourceBuilder, multiMatchQueryBuilder, TITLES_INDEX);
    }

    public List<Map> crossFields() throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.fetchSource(new String[]{"*"}, Strings.EMPTY_ARRAY);
        MultiMatchQueryBuilder multiMatchQueryBuilder =
                QueryBuilders.multiMatchQuery("Poland Street W1V", "street",
                        "city", "country", "postcode")
                        .operator(Operator.AND)
                        .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);
        return getMaps(sourceBuilder, multiMatchQueryBuilder, ADDRESS_INDEX);
    }

    private List<Map> getMaps(SearchSourceBuilder sourceBuilder,
                              MultiMatchQueryBuilder multiMatchQueryBuilder,
                              String address) throws IOException {
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.should(multiMatchQueryBuilder);
        sourceBuilder.query(boolBuilder);
        SearchRequest searchRequest = new SearchRequest(address);
        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest,
                RequestOptions.DEFAULT);
        return ESUtils.buildListMapResp(response);
    }
}
