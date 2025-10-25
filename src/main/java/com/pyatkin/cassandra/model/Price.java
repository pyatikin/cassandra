package com.pyatkin.cassandra.model;

import java.math.BigDecimal;

public class Price {
    private String priceId;
    private BigDecimal value;
    private String description;

    public Price() {
    }

    public Price(String priceId, BigDecimal value, String description) {
        this.priceId = priceId;
        this.value = value;
        this.description = description;
    }

    public String getPriceId() {
        return priceId;
    }

    public void setPriceId(String priceId) {
        this.priceId = priceId;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}

