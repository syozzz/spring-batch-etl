package com.syozzz.etl.builder;

import javax.sql.DataSource;
import java.util.Map;

public interface DataSourceBuilder {

    void build(Map<String, Object> params);

    DataSource get();

    void close();

}
