package com.pyatkin.cassandra.config;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.pyatkin.cassandra.model.Price;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class PriceCodec extends MappingCodec<UdtValue, Price> {

    private final UserDefinedType userType;

    public PriceCodec(@NonNull TypeCodec<UdtValue> innerCodec, @NonNull UserDefinedType userType) {
        super(innerCodec, GenericType.of(Price.class));
        this.userType = userType;
    }

    @NonNull
    @Override
    public UserDefinedType getCqlType() {
        return userType;
    }

    @NonNull
    @Override
    public GenericType<Price> getJavaType() {
        return GenericType.of(Price.class);
    }

    @Nullable
    @Override
    protected Price innerToOuter(@Nullable UdtValue value) {
        if (value == null) {
            return null;
        }
        Price price = new Price();
        price.setPriceId(value.getString("price_id"));
        price.setValue(value.getBigDecimal("value"));
        price.setDescription(value.getString("description"));
        return price;
    }

    @Nullable
    @Override
    protected UdtValue outerToInner(@Nullable Price value) {
        if (value == null) {
            return null;
        }
        return userType.newValue()
                .setString("price_id", value.getPriceId())
                .setBigDecimal("value", value.getValue())
                .setString("description", value.getDescription());
    }
}