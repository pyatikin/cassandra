package com.pyatkin.cassandra.service;

import com.pyatkin.cassandra.dao.ProductDao;
import com.pyatkin.cassandra.model.ProductDto;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class ProductService {
    private final ProductDao dao;

    public ProductService(ProductDao dao) {
        this.dao = dao;
    }

    public UUID save(ProductDto product) {
        return dao.save(product);
    }

    public Optional<ProductDto> get(UUID id) {
        return dao.get(id);
    }

    public List<ProductDto> listAll(int limit) {
        return dao.listAll(limit);
    }
}
