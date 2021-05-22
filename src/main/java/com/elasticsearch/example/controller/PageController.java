package com.elasticsearch.example.controller;

import com.elasticsearch.example.service.PageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/page")
public class PageController {

    @Autowired
    private PageService pageService;

    @GetMapping("/from-size")
    public List<Map> fromSize() throws IOException {
        return pageService.fromSize();
    }

    @GetMapping("/search-after")
    public List<Map> searchAfter() throws IOException {
        return pageService.searchAfter();
    }

    @GetMapping("/scroll")
    public List<Map> scroll() throws IOException {
        return pageService.scroll();
    }
}
