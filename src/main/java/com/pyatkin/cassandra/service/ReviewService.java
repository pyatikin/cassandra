package com.pyatkin.cassandra.service;

import com.pyatkin.cassandra.dao.ReviewDao;
import com.pyatkin.cassandra.model.ReviewDto;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class ReviewService {
    private final ReviewDao dao;

    public ReviewService(ReviewDao dao) {
        this.dao = dao;
    }

    public UUID save(ReviewDto review) {
        return dao.save(review);
    }

    public Optional<ReviewDto> get(UUID reviewId) {
        return dao.get(reviewId);
    }

    public List<ReviewDto> list(UUID productId, int limit) {
        return dao.list(productId, limit);
    }

    public boolean delete(UUID reviewId) {
        return dao.delete(reviewId);
    }
}
