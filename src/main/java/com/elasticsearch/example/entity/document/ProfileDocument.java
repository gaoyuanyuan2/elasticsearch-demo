package com.elasticsearch.example.entity.document;

import com.elasticsearch.example.entity.model.Technologies;
import lombok.Data;
import java.util.List;

@Data
public class ProfileDocument {

    private String id;
    private String firstName;
    private String lastName;
    private List<Technologies> technologies;
    private Location location;
    private List<String> emails;

}
