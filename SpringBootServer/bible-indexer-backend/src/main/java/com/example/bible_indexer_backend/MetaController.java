package com.example.bible_indexer_backend;

import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

@RestController
public class MetaController {

    @GetMapping("/books")
    public String getBooksJson() throws Exception {
        ClassPathResource resource = new ClassPathResource("books_meta.json");

        return StreamUtils.copyToString(
                resource.getInputStream(),
                StandardCharsets.UTF_8
        );
    }
}