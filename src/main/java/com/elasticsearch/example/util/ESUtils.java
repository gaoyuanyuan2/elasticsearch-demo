package com.elasticsearch.example.util;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 工具类
 */
public interface ESUtils {

    static List<Map> buildListMapResp(SearchTemplateResponse response) {
        List<Map> mapList = new ArrayList<>();
        Arrays.stream(response.getResponse().getHits().getHits()).forEach(e -> {
            Map<String, Object> resultMap = e.getSourceAsMap();
            mapList.add(resultMap);
        });
        return mapList;
    }

    static List<Map> buildListMapResp(SearchResponse response) {
        List<Map> mapList = new ArrayList<>();
        Arrays.stream(response.getHits().getHits()).forEach(e -> {
            Map<String, Object> resultMap = e.getSourceAsMap();
            mapList.add(resultMap);
        });
        return mapList;
    }

    static List<Map> buildListMapResp(MultiSearchTemplateResponse multiResponse) {
        List<Map> mapList = new ArrayList<>();
        Arrays.stream(multiResponse.getResponses()).forEach(e -> {
            Arrays.stream(e.getResponse().getResponse().getHits().getHits()).forEach(
                    e2 -> {
                        Map<String, Object> resultMap = e2.getSourceAsMap();
                        mapList.add(resultMap);
                    }
            );
        });
        return mapList;
    }

    static List<String> buildCompletionSuggestListResp(String suggesterName,
                                                       SearchResponse response) {
        List<String> textList = new ArrayList<>();
        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestion = suggest.getSuggestion(suggesterName);
        suggestion.getEntries().stream().forEach(options -> options.forEach(option -> textList.add(option.getText().toString())));
        return textList;

    }

    static List<String> buildTermSuggestListResp(String suggestName, SearchResponse response) {
        List<String> textList = new ArrayList<>();
        Suggest suggest = response.getSuggest();
        TermSuggestion termSuggestion = suggest.getSuggestion(suggestName);
        termSuggestion.getEntries().stream().forEach(options -> options.forEach(option -> textList.add(option.getText().toString())));
        return textList;
    }

    static List<String> buildPhraseSuggestListResp(String suggestName, SearchResponse response) {
        List<String> textList = new ArrayList<>();
        Suggest suggest = response.getSuggest();
        PhraseSuggestion termSuggestion = suggest.getSuggestion(suggestName);
        termSuggestion.getEntries().stream().forEach(options -> options.forEach(option -> textList.add(option.getText().toString())));
        return textList;
    }
}
