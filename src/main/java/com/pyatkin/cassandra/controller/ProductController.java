package com.pyatkin.cassandra.controller;

import com.pyatkin.cassandra.model.ProductDto;
import com.pyatkin.cassandra.service.ProductService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
public class ProductController {
    private final ProductService service;

    public ProductController(ProductService service) {
        this.service = service;
    }

    @PostMapping("/put/card")
    public ResponseEntity<Map<String, String>> create(@RequestBody ProductDto dto) {
        UUID id = service.save(dto);
        return ResponseEntity.ok(Map.of("productId", id.toString()));
    }

    @GetMapping("/get/card")
    public ResponseEntity<ProductDto> get(@RequestParam UUID productId) {
        return service.get(productId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    @GetMapping("/list/cards")
    public ResponseEntity<List<ProductDto>> list(@RequestParam(defaultValue = "100") int limit) {
        return ResponseEntity.ok(service.listAll(limit));
    }
}