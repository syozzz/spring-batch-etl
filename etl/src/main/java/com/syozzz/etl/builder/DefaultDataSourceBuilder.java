package com.syozzz.etl.builder;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;

//todo
@Component
@ConditionalOnMissingBean(DataSourceBuilder.class)
public class DefaultDataSourceBuilder implements DataSourceBuilder {



    @Override
    public void build(Map<String, Object> params) {

    }

    @Override
    public DataSource get() {
        return null;
    }

    @Override
    public void close() {

    }
}
