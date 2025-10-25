package com.pyatkin.cassandra.dao;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.pyatkin.cassandra.model.Price;
import com.pyatkin.cassandra.model.ProductDto;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ProductDao {
    private static final String PRODUCTS_PARTITION = "all"; // Единая партиция для всех продуктов
    private final CqlSession session;
    private final String keyspace;
    private final PreparedStatement insertProduct;
    private final PreparedStatement insertProductList;
    private final PreparedStatement selectProduct;
    private final PreparedStatement selectAllProducts;

    public ProductDao(CqlSession session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;

        insertProduct = session.prepare(
                "INSERT INTO " + keyspace + ".products (product_id, name, photos, prices, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)");
        insertProductList = session.prepare(
                "INSERT INTO " + keyspace + ".products_list (partition_key, created_at, product_id, name) VALUES (?, ?, ?, ?)");
        selectProduct = session.prepare(
                "SELECT * FROM " + keyspace + ".products WHERE product_id = ?");
        selectAllProducts = session.prepare(
                "SELECT product_id, name, created_at FROM " + keyspace + ".products_list WHERE partition_key = ? LIMIT ?");
    }

    public UUID save(ProductDto product) {
        UUID id = product.getProductId() != null ? product.getProductId() : UUID.randomUUID();
        Instant now = Instant.now();
        Instant createdAt = product.getCreatedAt() != null ? product.getCreatedAt() : now;

        // Используем batch для атомарной записи в обе таблицы
        BatchStatement batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                .addStatement(insertProduct.bind(
                        id,
                        product.getName(),
                        product.getPhotos(),
                        product.getPrices(),
                        createdAt,
                        now
                ))
                .addStatement(insertProductList.bind(
                        PRODUCTS_PARTITION,
                        createdAt,
                        id,
                        product.getName()
                ))
                .build();

        session.execute(batch);
        return id;
    }

    public Optional<ProductDto> get(UUID productId) {
        Row row = session.execute(selectProduct.bind(productId)).one();
        if (row == null) return Optional.empty();

        ProductDto dto = new ProductDto();
        dto.setProductId(productId);
        dto.setName(row.getString("name"));
        dto.setPhotos(row.getList("photos", String.class));
        dto.setPrices(row.getList("prices", Price.class));
        dto.setCreatedAt(row.getInstant("created_at"));
        dto.setUpdatedAt(row.getInstant("updated_at"));
        return Optional.of(dto);
    }

    public List<ProductDto> listAll(int limit) {
        ResultSet rs = session.execute(selectAllProducts.bind(PRODUCTS_PARTITION, limit));
        List<ProductDto> list = new ArrayList<>();

        for (Row row : rs) {
            ProductDto dto = new ProductDto();
            dto.setProductId(row.getUuid("product_id"));
            dto.setName(row.getString("name"));
            dto.setCreatedAt(row.getInstant("created_at"));
            list.add(dto);
        }

        return list;
    }
}