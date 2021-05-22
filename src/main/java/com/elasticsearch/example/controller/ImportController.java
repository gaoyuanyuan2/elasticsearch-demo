package com.elasticsearch.example.controller;

import com.elasticsearch.example.service.ImportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/import")
public class ImportController {

    @Autowired
    private ImportService importService;

    @GetMapping("/movies")
    public boolean movies() throws IOException {
        importService.movies();
        return true;
    }

    @GetMapping("/tmdb")
    public boolean tmdb() throws IOException {
        importService.tmdb();
        return true;
    }
}
