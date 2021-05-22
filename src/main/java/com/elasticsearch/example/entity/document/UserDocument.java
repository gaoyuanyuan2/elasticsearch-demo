package com.elasticsearch.example.entity.document;


import lombok.Data;

@Data
public class UserDocument {
    private String id;
    private String user;
    private String post_date;
    private String message;

}
