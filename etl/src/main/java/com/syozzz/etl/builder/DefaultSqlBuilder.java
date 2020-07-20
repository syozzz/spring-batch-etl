package com.syozzz.etl.builder;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConditionalOnMissingBean(SqlBuilder.class)
public class DefaultSqlBuilder implements SqlBuilder {

    public static final String TABLE_NAME = "tableName";
    public static final String SELECT_COLUMNS = "selectColumns";
    public static final String INSERT_COLUMNS = "insertColumns";
    public static final String WHERE_CLAUSE = "whereClause";

    @Override
    public String generateSql(SqlType sqlType, Map<String, Object> params) throws JSQLParserException {
        switch (sqlType) {
            case SELECT:
                return buildSelectSql(params);
            case INSERT:
                return buildInsertSql(params);
            default:
                throw new RuntimeException("not supported type, it is only in [SELECT, INSERT]");
        }
    }

    private String buildInsertSql(Map<String, Object> params) {
        Assert.notNull(params.get(TABLE_NAME));
        Assert.notNull(params.get(SELECT_COLUMNS));
        Assert.notNull(params.get(INSERT_COLUMNS));
        String sql = " insert into {}({}), values ({}) ";
        return StrUtil.format(sql, params.get(TABLE_NAME).toString(), params.get(SELECT_COLUMNS).toString(), params.get(INSERT_COLUMNS).toString());
    }

    private String buildSelectSql(Map<String, Object> params) throws JSQLParserException {
        Assert.notNull(params.get(TABLE_NAME));
        Assert.notNull(params.get(SELECT_COLUMNS));
        String[] strs = params.get(SELECT_COLUMNS).toString().split(",");
        Column[] columns = new Column[strs.length];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = new Column(strs[i]);
        }
        Select select = SelectUtils.buildSelectFromTableAndExpressions(new Table(params.get(TABLE_NAME).toString()), columns);
        if (params.containsKey(WHERE_CLAUSE)) {
            Object whw = params.get(WHERE_CLAUSE);
            if (null != whw) {
                Expression where = CCJSqlParserUtil.parseCondExpression(whw.toString());
                SelectUtils.addExpression(select, where);
            }
        }
        return select.toString();
    }

}
