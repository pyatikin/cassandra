package com.pyatkin.cassandra.controller;

import com.pyatkin.cassandra.model.ReviewDto;
import com.pyatkin.cassandra.service.ReviewService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
public class ReviewController {
    private final ReviewService service;

    public ReviewController(ReviewService service) {
        this.service = service;
    }

    @PostMapping("/put/review")
    public ResponseEntity<Map<String, String>> add(@RequestBody ReviewDto dto) {
        UUID id = service.save(dto);
        return ResponseEntity.ok(Map.of("reviewId", id.toString()));
    }

    @GetMapping("/get/review")
    public ResponseEntity<ReviewDto> get(@RequestParam UUID reviewId) {
        return service.get(reviewId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    @GetMapping("/list/reviews")
    public ResponseEntity<List<ReviewDto>> list(@RequestParam UUID productId,
                                                @RequestParam(defaultValue = "20") int limit) {
        return ResponseEntity.ok(service.list(productId, limit));
    }

    @DeleteMapping("/delete/review")
    public ResponseEntity<Void> delete(@RequestParam UUID reviewId) {
        boolean ok = service.delete(reviewId);
        return ok ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }
}