package com.elasticsearch.example.controller;

import com.elasticsearch.example.service.SuggestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/suggest")
public class SuggestController {
    @Autowired
    private SuggestService suggestService;

    @GetMapping("/completion-suggest-search")
    public List<String> completionSuggestSearch(@RequestParam String prefix) throws IOException {
        return suggestService.completionSuggestSearch(prefix);
    }

    @GetMapping("/term-suggest")
    public Map<String, List<String>> termSuggest(@RequestParam String text1,
                                                 @RequestParam String text2) throws IOException {
        return suggestService.termSuggest(text1, text2);
    }

    @GetMapping("/phrase-suggest")
    public List<String> phraseSuggest() throws IOException {
        return suggestService.phraseSuggest();
    }

    @GetMapping("/context-suggest")
    public List<String> contextSuggest() throws IOException {
        return suggestService.contextSuggest();
    }
}
