package com.elasticsearch.example.entity.document;

import lombok.Data;

import java.util.List;

@Data
public class MoviesDocument {
    private String year;
    private String id;
    private String title;
    private List<String> genre;

}
