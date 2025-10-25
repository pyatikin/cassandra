package com.pyatkin.cassandra.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

@Configuration
public class CassandraConfig {

    @Value("${cassandra.config-path}")
    private String configPath;

    private Map<String, String> cfg;
    private CqlSession session;

    @PostConstruct
    public void init() throws IOException {
        this.cfg = ConfigReader.read(configPath);
    }

    @Bean(destroyMethod = "close")
    public CqlSession cassandraSession() {
        if (cfg == null) throw new IllegalStateException("Cassandra config not loaded");

        String contactPointsStr = cfg.get("contactpoint");
        String user = cfg.get("user");
        String password = cfg.get("password");
        String keyspace = cfg.get("keyspace");

        // Поддержка множественных contact points
        String[] contactPoints = contactPointsStr.split(",");

        var builder = CqlSession.builder()
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(user, password);

        for (String cp : contactPoints) {
            builder.addContactPoint(new InetSocketAddress(cp.trim(), 9042));
        }

        session = builder.build();

        // Создаем keyspace и таблицы
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
        session.execute("USE " + keyspace);

        // Создаем UDT для price
        session.execute("CREATE TYPE IF NOT EXISTS " + keyspace + ".price (" +
                "price_id text, value decimal, description text)");

        // Основная таблица товаров (по product_id)
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".products (" +
                "product_id uuid PRIMARY KEY, " +
                "name text, photos list<text>, prices list<frozen<price>>, " +
                "created_at timestamp, updated_at timestamp)");

        // Дополнительная таблица для получения списка всех товаров
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".products_list (" +
                "partition_key text, " +
                "created_at timestamp, " +
                "product_id uuid, " +
                "name text, " +
                "PRIMARY KEY (partition_key, created_at, product_id)) " +
                "WITH CLUSTERING ORDER BY (created_at DESC, product_id DESC)");

        // Таблица отзывов по продукту (для списка)
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".reviews_by_product (" +
                "product_id uuid, review_time timestamp, review_id uuid, user_id uuid, " +
                "text text, media list<text>, " +
                "PRIMARY KEY (product_id, review_time, review_id)) " +
                "WITH CLUSTERING ORDER BY (review_time DESC, review_id DESC)");

        // Таблица отзывов по ID (для получения одного отзыва и удаления)
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".reviews_by_id (" +
                "review_id uuid PRIMARY KEY, product_id uuid, user_id uuid, " +
                "review_time timestamp, text text, media list<text>)");

        // Регистрируем codec для Price UDT
        UserDefinedType priceUdt = session.getMetadata()
                .getKeyspace(keyspace)
                .flatMap(ks -> ks.getUserDefinedType("price"))
                .orElseThrow();

        MutableCodecRegistry codecRegistry = (MutableCodecRegistry) session.getContext().getCodecRegistry();
        codecRegistry.register(new PriceCodec(TypeCodecs.udtOf(priceUdt), priceUdt));


        return session;
    }

    @Bean
    public String keyspace() {
        return cfg.get("keyspace");
    }
}