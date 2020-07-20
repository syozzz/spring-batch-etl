package com.syozzz.etl.builder;


import java.util.Map;

public interface SqlBuilder {

    String generateSql(SqlType sqlType, Map<String, Object> params) throws Exception;

}
