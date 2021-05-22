package com.elasticsearch.example.controller;

import com.elasticsearch.example.service.FlightsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("flights")
public class FlightsController {
    @Autowired
    private FlightsService flightsService;

    @GetMapping("stat-max")
    public Map statMax() throws IOException {
        return flightsService.statMax();
    }

    @GetMapping("term-destCountry")
    public Map termDestCountryTerms() throws IOException {
        return flightsService.termDestCountry();
    }
}
