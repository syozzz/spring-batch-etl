package com.syozzz.etl.builder;

import cn.hutool.core.lang.Assert;
import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
@ConditionalOnMissingBean(DataSourceBuilder.class)
public class DefaultDataSourceBuilder implements DataSourceBuilder {

    private final static ThreadLocal<DruidDataSource> SOURCE = new ThreadLocal<>();
    private final static ThreadLocal<DruidDataSource> TARGET = new ThreadLocal<>();

    public void setSource(DruidDataSource source) {
        SOURCE.set(source);
    }

    public void setTarget(DruidDataSource target) {
        TARGET.set(target);
    }

    @Override
    public DataSource getSourceOne() {
        DataSource d = SOURCE.get();
        Assert.notNull(d);
        return d;
    }

    @Override
    public DataSource getTargetOne() {
        DataSource d = TARGET.get();
        Assert.notNull(d);
        return d;
    }

    @Override
    public void close() {
        SOURCE.get().close();
        TARGET.get().close();
        SOURCE.remove();
        TARGET.remove();
    }
}
