package com.elasticsearch.example.controller;

import com.elasticsearch.example.service.ScriptQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/script")
public class ScriptQueryController {

    @Autowired
    private ScriptQueryService scriptQueryService;

    @GetMapping("/inline-template")
    public List<Map> inlineTemplate() throws IOException {
        return scriptQueryService.inlineTemplate();
    }

    @GetMapping("/register-template")
    public int registerTemplate() throws IOException {
        return scriptQueryService.registerTemplate();
    }

    @GetMapping("/run-register-template")
    public List<Map> runRegisterTemplate() throws IOException {
        return scriptQueryService.runRegisterTemplate();
    }

    @GetMapping("/multi-template")
    public List<Map> multiTemplate() throws IOException {
        return scriptQueryService.multiTemplate();
    }
}
