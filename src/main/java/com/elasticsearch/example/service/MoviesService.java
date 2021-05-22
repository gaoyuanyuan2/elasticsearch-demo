package com.elasticsearch.example.service;

import com.elasticsearch.example.entity.document.MoviesDocument;
import com.elasticsearch.example.entity.document.UserDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static com.elasticsearch.example.util.Constant.MOVIES_INDEX;

@Service
public class MoviesService {

    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private ObjectMapper objectMapper;

    public List<MoviesDocument> mustShouldRandSearch() throws IOException {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.fetchSource(new String[]{"*"}, Strings.EMPTY_ARRAY);
// 打分
//        MatchQueryBuilder boostingBuilder = new MatchQueryBuilder("title", "Sensibility");
//        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("year", "2021");
//        BoostingQueryBuilder boostingQueryBuilder = new BoostingQueryBuilder(boostingBuilder,termQueryBuilder);
//        boostingQueryBuilder.negativeBoost(0.2f);
        // 全文匹配，会被es解析并进行分词
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery
                ("title", "Sense");

        // 多字段全文匹配
        // DEFAULT_TYPE = MultiMatchQueryBuilder.Type.BEST_FIELDS
        /**
         * * 最佳字段(Best Fields)
         *   是默认类型，可以不⽤指定
         *   当字段之间相互竞争，又相互关联。例如title和body这样的字段。评分来自最匹配字段
         * * 多数字段 (Most Fields)
         *   处理英文内容时:一种常见的手段是，在主字段( English Analyzer),
         *   抽取词干，加入同义词，以匹配更多的文档。相同的文本，加入子字段(Standard Analyzer),
         *   以提供更加精确的匹配。其他字段作为匹配文档提高相关度的信号。匹配字段越多则越好
         * * 混合字段(Cross Field)
         *   对于某些实体，例如人名，地址，图书信息。需要在多个字段中确定信息，单个字段只能作为整体的一部分。希望在任何这些列出的字段中找到尽可能多的词
         */
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders
       .multiMatchQuery("1995", "title", "year").tieBreaker(0.1f).type(MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // 它仅匹配在给定字段中含有该词条的文档，而且是确切的、未经分析的词条。term查询会查找我们设定的准确值。
        // 使用Term(s)QueryBuilder查询内容需要转换成全小写，而MatchQueryBuilder不需要转换。
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery
                ("year", "1995");

        // 范围查找
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("id");
        rangeQueryBuilder.from("10");
        rangeQueryBuilder.to("20");

        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(matchQueryBuilder);
        boolBuilder.must(termQueryBuilder);
        boolBuilder.must(rangeQueryBuilder);
        boolBuilder.should(multiMatchQueryBuilder);
        sourceBuilder.query();


        sourceBuilder.query(boolBuilder);
        SearchRequest searchRequest = new SearchRequest(MOVIES_INDEX);
        searchRequest.source(sourceBuilder);
        List<MoviesDocument> moviesDocumentList = new ArrayList<>();
        try {
            SearchResponse response = client.search(searchRequest,
                    RequestOptions.DEFAULT);
            Arrays.stream(response.getHits().getHits()).forEach(e -> {
                Map<String, Object> resultMap = e.getSourceAsMap();
                moviesDocumentList.add(objectMapper.convertValue(resultMap,
                        MoviesDocument.class));

            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return moviesDocumentList;
    }
}
