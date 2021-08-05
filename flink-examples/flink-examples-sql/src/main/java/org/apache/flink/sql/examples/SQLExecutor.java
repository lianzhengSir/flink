package org.apache.flink.sql.examples;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Optional;


/**
 * @author zlz
 * @Description
 * @since 2021/8/5 15:22
 */
public class SQLExecutor {
    private StreamTableEnvironment tableEnvironment;
    private static final char SEMICOLON = ';';

    public SQLExecutor(StreamTableEnvironment tableEnvironment) {
        this.tableEnvironment = tableEnvironment;
    }

    public void execute(String sqlList) throws Exception {
        char[] chars = sqlList.toCharArray();
        final StringBuilder sql = new StringBuilder();
        for (char ch : chars) {
            if (SEMICOLON == ch) {
                this.callCommand(sql.toString());
                sql.setLength(0);
            } else {
                sql.append(ch);
            }
        }
    }

    public void callCommand(String sql) {
        if (StringUtils.isBlank(sql)) {
            return;
        }
        //注冊udf
        Optional<SqlCommandParser.SqlCommandCall> parsedLine = SqlCommandParser.parse(sql);
        parsedLine.map(cmdCall -> {
            this.callCommand(cmdCall);
            return true;
        });
    }

    private void callUseCatalog(SqlCommandParser.SqlCommandCall cmdCall) {
        tableEnvironment.useCatalog(cmdCall.operands[0]);
    }

    private void callUseDatabase(SqlCommandParser.SqlCommandCall cmdCall) {
        tableEnvironment.useDatabase(cmdCall.operands[0]);
    }

    private void callInsert(SqlCommandParser.SqlCommandCall cmdCall) {
        tableEnvironment.sqlUpdate(cmdCall.operands[0]);
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        tableEnvironment.sqlUpdate(cmdCall.operands[0]);
    }

    private void callCreateView(SqlCommandParser.SqlCommandCall cmdCall) {
        tableEnvironment.createTemporaryView(cmdCall.operands[0], tableEnvironment.sqlQuery(cmdCall.operands[1]));
    }

    private void callCreateDatabase(SqlCommandParser.SqlCommandCall cmdCall) {
        tableEnvironment.sqlUpdate(cmdCall.operands[0]);
    }

    private void callFunction(SqlCommandParser.SqlCommandCall cmdCall) {
        try {
            Class<?> classType = Class.forName(cmdCall.operands[1]);
            Object function = classType.getConstructor().newInstance();
            if (function instanceof AggregateFunction) {
                tableEnvironment.registerFunction(cmdCall.operands[0], (AggregateFunction) function);
            } else if (function instanceof TableAggregateFunction) {
                tableEnvironment.registerFunction(cmdCall.operands[0], (TableAggregateFunction) function);
            } else if (function instanceof TableFunction) {
                tableEnvironment.registerFunction(cmdCall.operands[0], (TableFunction) function);
            } else if (function instanceof ScalarFunction) {
                tableEnvironment.registerFunction(cmdCall.operands[0], (ScalarFunction) function);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case USE_CATALOG:
                callUseCatalog(cmdCall);
                break;
            case USE:
                callUseDatabase(cmdCall);
                break;
            case INSERT_INTO:
            case INSERT_OVERWRITE:
                callInsert(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case CREATE_VIEW:
                callCreateView(cmdCall);
                break;
            case CREATE_DATABASE:
                callCreateDatabase(cmdCall);
                break;
            case CREATE_FUNCTION:
                callFunction(cmdCall);
                break;
            default:
                throw new SqlClientException("Unsupported command: " + cmdCall.command);
        }
    }
}
