package com.elasticsearch.example.controller;

import com.elasticsearch.example.entity.document.UserDocument;
import com.elasticsearch.example.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/user")
public class UserController {

    @Autowired
    private UserService userService;

    @PostMapping("/create")
    public ResponseEntity create(@RequestBody UserDocument userDocument) throws Exception {
        return new ResponseEntity(userService.create(userDocument),
                HttpStatus.CREATED);
    }

    @GetMapping("/get")
    public UserDocument findById(@RequestParam String id) throws Exception {
        return userService.findById(id);
    }

    @PutMapping("update")
    public ResponseEntity update(@RequestBody UserDocument userDocument) throws Exception {

        return new ResponseEntity(userService.update(userDocument),
                HttpStatus.CREATED);
    }

    @GetMapping("/all")
    public List<UserDocument> findAll() throws Exception {

        return userService.findAll();
    }

    @GetMapping(value = "/name-search")
    public List<UserDocument> searchByName(@RequestParam(value = "name") String name) throws Exception {
        return userService.searchByName(name);
    }


    @DeleteMapping("/delete")
    public String delete(@RequestParam String id) throws Exception {

        return userService.deleteById(id);

    }
}
