package com.syozzz.etl.builder;

import javax.sql.DataSource;

public interface DataSourceBuilder {

    DataSource getSourceOne();

    DataSource getTargetOne();

    void close();

}
