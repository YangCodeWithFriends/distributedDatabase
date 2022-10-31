package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.SQLEnum;
import common.Transaction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:36 AM
 */
public class TopBalanceTransaction extends Transaction {
    @Override
    protected void YCQLExecute(CqlSession cqlSession, Logger logger) {
        logger.log(Level.INFO, "Begin YCQL TOP Balance");
        ResultSet rs = null;
        List<Row> rows = null;
        SimpleStatement simpleStatement = null;

        Timestamp current_time = Timestamp.from(Instant.now());
        logger.log(Level.INFO, "Get current timestamp = " + current_time);
        for (int C_W_ID = 1; C_W_ID <= 10; C_W_ID++) {
            for (int C_D_ID = 1; C_D_ID <= 10; C_D_ID++) {

                // CQL2
                String CQL2 = String.format("select C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from dbycql.customer " +
                        "where C_W_ID = %d and C_D_ID = %d limit 10;", C_W_ID, C_D_ID);
                rs = cqlSession.execute(CQL2);
                rows = rs.all();
                for (Row row : rows) {
//                    int C_W_ID = row.getInt(0);
//                    int C_D_ID = row.getInt(1);
                    int C_ID = row.getInt(2);
                    String C_FIRST = row.getString(3);
                    String C_MIDDLE = row.getString(4);
                    String C_LAST = row.getString(5);
                    BigDecimal C_BALANCE = row.getBigDecimal(6);

                    // CQL3
                    String CQL3 = String.format("insert into dbycql.customer_balance_top10 (CB_TIME_GROUP, CB_W_ID, CB_D_ID, CB_ID, CB_FIRST, CB_MIDDLE, CB_LAST, CB_BALANCE, CB_TIME_ADD) " +
                            "values ('%s', %d, %d, %d, '%s', '%s', '%s', %f,now());", current_time, C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE);
                    simpleStatement = SimpleStatement.builder(CQL3)
                            .setExecutionProfileName("oltp")
                            .build();
                    cqlSession.execute(simpleStatement);
                }
            }
        }

        // CQL4
        String CQL4 = String.format("select CB_W_ID, CB_D_ID, CB_ID, CB_FIRST, CB_MIDDLE, CB_LAST, CB_BALANCE from dbycql.customer_balance_top10 " +
                "where CB_TIME_GROUP = '%s' limit 10;", current_time);
        rs = cqlSession.execute(CQL4);
        rows = rs.all();
        for (Row row : rows) {
            int C_W_ID = row.getInt(0);
            int C_D_ID = row.getInt(1);
            int C_ID = row.getInt(2);
            String C_FIRST = row.getString(3);
            String C_MIDDLE = row.getString(4);
            String C_LAST = row.getString(5);
            BigDecimal C_BALANCE = row.getBigDecimal(6);

            /*
            // CQL5
            String CQL5 = String.format("select W_NAME from dbycql.Warehouse where W_ID = %d;", C_W_ID);
            rs = cqlSession.execute(CQL5);
            String W_NAME = rs.one().getString(0);

            // CQL6
            String CQL6 = String.format("select D_NAME from dbycql.District where D_W_ID = %d and D_ID = %d;", C_W_ID, C_D_ID);
            rs = cqlSession.execute(CQL6);
            String D_NAME = rs.one().getString(0);
           logger.log(Level.INFO, String.format("C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s,C_BALANCE=%f,W_NAME=%s,D_NAME=%s\n", C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME));

             */
        }

        // CQL7
        String CQL7 = String.format("delete from dbycql.customer_balance_top10 where CB_TIME_GROUP = '%s';",current_time);
        simpleStatement = SimpleStatement.builder(CQL7)
                .setExecutionProfileName("oltp")
                .build();
        cqlSession.execute(simpleStatement);
        logger.log(Level.INFO, "Top Balance ends");
    }

    @Override
    protected void YSQLExecute(Connection conn, Logger logger) throws SQLException {
        conn.setAutoCommit(false);
        try {
            java.sql.ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.TopBalanceTransaction1.SQL));
            while (rs.next()) {
                String C_FIRST = rs.getString(1);
                String C_MIDDLE = rs.getString(2);
                String C_LAST = rs.getString(3);
                double C_BALANCE = rs.getDouble(4);
                String W_NAME = rs.getString(5);
                String D_NAME = rs.getString(6);
               logger.log(Level.INFO, String.format("C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s,C_BALANCE=%f,W_NAME=%s,D_NAME=%s\n", C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME));
            }
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.log(Level.SEVERE, String.format("Error in %s transaction, exception= ",getTransactionType().type),e);

            if (conn != null) {
//                System.err.print("Transaction is being rolled back\n");
                logger.log(Level.WARNING, "Transaction is being rolled back");
                conn.rollback();
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }
}
