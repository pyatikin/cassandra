package com.pyatkin.cassandra.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.pyatkin.cassandra.model.ReviewDto;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ReviewDao {
    private final CqlSession session;
    private final String keyspace;

    private final PreparedStatement insertReview;
    private final PreparedStatement insertReviewById;
    private final PreparedStatement selectReviewsByProduct;
    private final PreparedStatement selectReviewById;
    private final PreparedStatement deleteReviewFromProduct;
    private final PreparedStatement deleteReviewById;

    public ReviewDao(CqlSession session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;

        insertReview = session.prepare(
                "INSERT INTO " + keyspace + ".reviews_by_product (product_id, review_time, review_id, user_id, text, media) VALUES (?, ?, ?, ?, ?, ?)");
        insertReviewById = session.prepare(
                "INSERT INTO " + keyspace + ".reviews_by_id (review_id, product_id, user_id, review_time, text, media) VALUES (?, ?, ?, ?, ?, ?)");
        selectReviewsByProduct = session.prepare(
                "SELECT * FROM " + keyspace + ".reviews_by_product WHERE product_id = ? LIMIT ?");
        selectReviewById = session.prepare(
                "SELECT * FROM " + keyspace + ".reviews_by_id WHERE review_id = ?");
        deleteReviewFromProduct = session.prepare(
                "DELETE FROM " + keyspace + ".reviews_by_product WHERE product_id = ? AND review_time = ? AND review_id = ?");
        deleteReviewById = session.prepare(
                "DELETE FROM " + keyspace + ".reviews_by_id WHERE review_id = ?");
    }

    public UUID save(ReviewDto review) {
        UUID reviewId = UUID.randomUUID();
        Instant reviewTime = Instant.now();

        BatchStatement batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                .addStatement(insertReview.bind(
                        review.getProductId(), reviewTime, reviewId, review.getUserId(), review.getText(), review.getMedia()))
                .addStatement(insertReviewById.bind(
                        reviewId, review.getProductId(), review.getUserId(), reviewTime, review.getText(), review.getMedia()))
                .build();
        session.execute(batch);
        return reviewId;
    }

    public Optional<ReviewDto> get(UUID reviewId) {
        Row row = session.execute(selectReviewById.bind(reviewId)).one();
        if (row == null) return Optional.empty();

        ReviewDto r = new ReviewDto();
        r.setReviewId(reviewId);
        r.setProductId(row.getUuid("product_id"));
        r.setUserId(row.getUuid("user_id"));
        r.setReviewTime(row.getInstant("review_time"));
        r.setText(row.getString("text"));
        r.setMedia(row.getList("media", String.class));
        return Optional.of(r);
    }

    public List<ReviewDto> list(UUID productId, int limit) {
        ResultSet rs = session.execute(selectReviewsByProduct.bind(productId, limit));
        List<ReviewDto> list = new ArrayList<>();
        for (Row row : rs) {
            ReviewDto r = new ReviewDto();
            r.setProductId(productId);
            r.setReviewId(row.getUuid("review_id"));
            r.setUserId(row.getUuid("user_id"));
            r.setReviewTime(row.getInstant("review_time"));
            r.setText(row.getString("text"));
            r.setMedia(row.getList("media", String.class));
            list.add(r);
        }
        return list;
    }

    public boolean delete(UUID reviewId) {
        Row row = session.execute(selectReviewById.bind(reviewId)).one();
        if (row == null) return false;

        UUID productId = row.getUuid("product_id");
        Instant reviewTime = row.getInstant("review_time");

        BatchStatement batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                .addStatement(deleteReviewFromProduct.bind(productId, reviewTime, reviewId))
                .addStatement(deleteReviewById.bind(reviewId))
                .build();
        session.execute(batch);
        return true;
    }
}