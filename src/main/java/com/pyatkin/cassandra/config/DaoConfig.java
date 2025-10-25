package com.pyatkin.cassandra.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.pyatkin.cassandra.dao.ProductDao;
import com.pyatkin.cassandra.dao.ReviewDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DaoConfig {

    private final String keyspace;

    public DaoConfig(String keyspace) {
        this.keyspace = keyspace;
    }

    @Bean
    public ProductDao productDao(CqlSession session) {
        return new ProductDao(session, keyspace);
    }

    @Bean
    public ReviewDao reviewDao(CqlSession session) {
        return new ReviewDao(session, keyspace);
    }
}