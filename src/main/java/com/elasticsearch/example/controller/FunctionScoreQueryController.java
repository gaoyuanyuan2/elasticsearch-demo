package com.elasticsearch.example.controller;

import com.elasticsearch.example.service.FunctionScoreQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/score")
public class FunctionScoreQueryController {
    @Autowired
    private FunctionScoreQueryService functionScoreQueryService;

    @GetMapping("/boost")
    public List<Map> boost() throws IOException {
        return functionScoreQueryService.boost();
    }

    @GetMapping("/boosting")
    public List<Map> boosting() throws IOException {
        return functionScoreQueryService.boosting();
    }
    @GetMapping("/function-score")
    public List<Map> functionScore() throws IOException {
        return functionScoreQueryService.functionScore();
    }

    @GetMapping("/field-value-factor")
    public List<Map> fieldValueFactor() throws IOException {
        return functionScoreQueryService.fieldValueFactor();
    }

    @GetMapping("/decay-function")
    public List<Map> decayFunction() throws IOException {
        return functionScoreQueryService.decayFunction();
    }
}
