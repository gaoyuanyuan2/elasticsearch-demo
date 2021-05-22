package com.elasticsearch.example.service;

import com.elasticsearch.example.entity.document.UserDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.*;

@Service
public class UserService {

    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private ObjectMapper objectMapper;


    public String create(UserDocument userDocument) throws IOException {
        IndexRequest indexRequest = new IndexRequest(USER_INDEX)
                .source(objectMapper.convertValue(userDocument, Map.class),
                        XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest,
                RequestOptions.DEFAULT);
        return indexResponse.getResult().name();
    }

    public Object update(UserDocument userDocument) throws Exception {

        UpdateRequest updateRequest = new UpdateRequest(USER_INDEX,
                userDocument.getId());

        updateRequest.doc(objectMapper.convertValue(userDocument, Map.class));

        UpdateResponse updateResponse = client.update(updateRequest,
                RequestOptions.DEFAULT);

        return updateResponse.getResult().name();
    }

    public UserDocument findById(String id) throws Exception {

        GetRequest getRequest = new GetRequest(USER_INDEX, id);

        GetResponse getResponse = client.get(getRequest,
                RequestOptions.DEFAULT);

        Map<String, Object> resultMap = getResponse.getSource();

        return objectMapper.convertValue(resultMap, UserDocument.class);

    }

    public List<UserDocument> findAll() throws IOException {

        SearchRequest searchRequest = new SearchRequest(USER_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse =
                client.search(searchRequest, RequestOptions.DEFAULT);

        return getSearchResult(searchResponse);
    }

    private List<UserDocument> getSearchResult(SearchResponse response) {

        SearchHit[] searchHit = response.getHits().getHits();

        List<UserDocument> profileDocuments = new ArrayList<>();

        for (SearchHit hit : searchHit) {
            profileDocuments.add(objectMapper
                    .convertValue(hit.getSourceAsMap(), UserDocument.class));
        }

        return profileDocuments;
    }

    public List<UserDocument> searchByName(String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(USER_INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder matchQueryBuilder = QueryBuilders
                .matchQuery("user", name)
                .operator(Operator.AND);

        searchSourceBuilder.query(matchQueryBuilder);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse =
                client.search(searchRequest, RequestOptions.DEFAULT);

        return getSearchResult(searchResponse);
    }

    public String deleteById(String id) throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest(USER_INDEX, id);

        DeleteResponse response = client.delete(deleteRequest,
                RequestOptions.DEFAULT);

        return response.getResult().name();
    }
}
