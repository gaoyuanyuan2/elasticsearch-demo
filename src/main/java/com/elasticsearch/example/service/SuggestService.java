package com.elasticsearch.example.service;

import com.elasticsearch.example.util.ESUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.elasticsearch.example.util.Constant.ARTICLES_INDEX;
import static com.elasticsearch.example.util.Constant.COMMENTS_INDEX;

/**
 * 自动补全与基于上下文的提示
 *
 * @author : Y
 * @since 2021/3/27 15:16
 */
@Service
public class SuggestService {
    @Autowired
    private RestHighLevelClient client;
    private static final String SUGGEST_01 = "my_suggestion_01";
    private static final String SUGGEST_02 = "my_suggestion_02";
    private static final String PHRASE_SUGGEST = "phrase-suggest";
    private static final String CONTEXT_SUGGEST = "context-suggest";

    /**
     * Completion：⾃动补全与基于上下⽂的提示
     *
     * @param prefix 前缀
     * @return
     * @throws IOException
     */
    public List<String> completionSuggestSearch(String prefix) throws IOException {

        SearchRequest request = new SearchRequest(ARTICLES_INDEX);

        //创建suggestion
        CompletionSuggestionBuilder completionSuggestionBuilder =
                SuggestBuilders.completionSuggestion("title_completion").prefix(prefix);

        //创建SuggestBuilder, 添加completionSuggestion
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("article-suggester",
                completionSuggestionBuilder);

        //加入source
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        sourceBuilder.suggest(suggestBuilder);
        request.source(sourceBuilder);

        //发送请求
        SearchResponse response = client.search(request,
                RequestOptions.DEFAULT);

        return ESUtils.buildCompletionSuggestListResp("article-suggester",
                response);
    }

    /**
     * 用户输入的 “lucen”是一个错误的拼写,会到指定的字段“body”上搜索，当无法搜索到结果时(missing),返回建议的词
     *
     * @param
     * @throws IOException
     */
    public Map<String, List<String>> termSuggest(String text1, String text2) throws IOException {
        //创建一个search请求
        SearchRequest request = new SearchRequest(ARTICLES_INDEX);

        //创建两个suggestion
        TermSuggestionBuilder termSuggestionBuilder1 =
                SuggestBuilders.termSuggestion("body").text(text1);
        termSuggestionBuilder1.suggestMode(TermSuggestionBuilder.SuggestMode.ALWAYS);

        TermSuggestionBuilder termSuggestionBuilder2 =
                SuggestBuilders.termSuggestion("body").text(text2);
        termSuggestionBuilder2.prefixLength(0);

        //创建SuggestBuilder, 添加两个termSuggestion
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(SUGGEST_01,
                termSuggestionBuilder1).addSuggestion(SUGGEST_02,
                termSuggestionBuilder2);

        //加入source
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        sourceBuilder.suggest(suggestBuilder);
        request.source(sourceBuilder);

        //发送请求
        SearchResponse response = client.search(request,
                RequestOptions.DEFAULT);
        Map<String, List<String>> map = new HashMap();
        map.put(SUGGEST_01, ESUtils.buildTermSuggestListResp(SUGGEST_01,
                response));
        map.put(SUGGEST_02, ESUtils.buildTermSuggestListResp(SUGGEST_02,
                response));
        return map;
    }


    /**
     * * Phrase Suggester在Term Suggester.上增加了一些额外的逻辑
     * * Suggest Mode : missing, popular, always
     * * Max Errors: 最多可以拼错的Terms数
     * * Confidence: 限制返回结果数，默认为1
     *
     * @return
     * @throws IOException
     */
    public List<String> phraseSuggest() throws IOException {
        //创建一个search请求
        SearchRequest request = new SearchRequest(ARTICLES_INDEX);

        //创建suggestion
        PhraseSuggestionBuilder phraseSuggestionBuilder =
                SuggestBuilders.phraseSuggestion("body")
                        .text("lucne and elasticsear rock hello world").maxErrors(2).highlight("<em>", "</em>");

        //创建SuggestBuilder, 添加phraseSuggestion
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(PHRASE_SUGGEST, phraseSuggestionBuilder);

        //加入source
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        sourceBuilder.suggest(suggestBuilder);
        request.source(sourceBuilder);

        //发送请求
        SearchResponse response = client.search(request,
                RequestOptions.DEFAULT);
        return ESUtils.buildPhraseSuggestListResp(PHRASE_SUGGEST,
                response);
    }


    /**
     * * Completion Suggester的扩展
     * * 可以在搜索中加入更多的上下文信息，例如，输入"star"
     * * 咖啡相关:建议"Starbucks"
     * * 电影相关: "star wars"
     *
     * @return
     * @throws IOException
     */
    public List<String> contextSuggest() throws IOException {
        //创建一个search请求
        SearchRequest request = new SearchRequest(COMMENTS_INDEX);

        //创建suggestion
        CompletionSuggestionBuilder completionSuggestionBuilder =
                SuggestBuilders.completionSuggestion("comment_autocomplete")
                        .prefix("star")
                        .contexts(Collections.<String, List<?
                                extends ToXContent>>singletonMap(
                                "comment_category",
                                Collections.singletonList(CategoryQueryContext.builder().setCategory("coffee").build())));


        //创建SuggestBuilder, 添加contextSuggestion
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(CONTEXT_SUGGEST,
                completionSuggestionBuilder);

        //加入source
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        sourceBuilder.suggest(suggestBuilder);
        request.source(sourceBuilder);

        //发送请求
        SearchResponse response = client.search(request,
                RequestOptions.DEFAULT);
        return ESUtils.buildCompletionSuggestListResp(CONTEXT_SUGGEST,
                response);
    }


}
