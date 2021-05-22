package com.elasticsearch.example.controller;

import com.elasticsearch.example.service.MultiMatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/multi-match")
public class MultiMatchController {

    @Autowired
    private MultiMatchService multiMatchService;

    @GetMapping("/most-fields")
    public List<Map> mostFields() throws IOException {
        return multiMatchService.mostFields();
    }

    @GetMapping("/cross-fields")
    public List<Map> crossFields() throws IOException {
        return multiMatchService.crossFields();
    }
}
