package com.elasticsearch.example.controller;

import com.elasticsearch.example.entity.document.MoviesDocument;
import com.elasticsearch.example.service.MoviesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/movies")
public class MoviesController {
    @Autowired
    private MoviesService moviesService;

    // 查询排序 AND OR NOT
    @GetMapping("/search")
    private List<MoviesDocument> search() throws IOException {
       return moviesService.mustShouldRandSearch();
    }

}
