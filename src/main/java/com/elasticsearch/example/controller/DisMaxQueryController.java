package com.elasticsearch.example.controller;

import com.elasticsearch.example.service.DisMaxQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dis-max")
public class DisMaxQueryController {

    @Autowired
    private DisMaxQueryService disMaxQueryService;
    @GetMapping("/dis-max")
    public List<Map> disMax(@RequestParam String title, @RequestParam String body,
                            @RequestParam(value = "tieBreaker", required = false, defaultValue = "0" ) float tieBreaker) throws Exception{
        return disMaxQueryService.disMax(title,body,tieBreaker);
    }
}
