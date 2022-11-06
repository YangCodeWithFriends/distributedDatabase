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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:19 AM
 */
// YSQL: 43151ms, YCQL: 17213ms
public class StockLevelTransaction extends Transaction {
    int W_ID;
    int D_ID;
    int T;
    int L;

    public StockLevelTransaction(int w_ID, int d_ID, int t, int l) {
        W_ID = w_ID;
        D_ID = d_ID;
        T = t;
        L = l;
    }

    @Override
    protected void YCQLExecute(CqlSession cqlSession, Logger logger) {
        ResultSet rs = null;
        List<Row> rows = null;
        SimpleStatement simpleStatement = null;

        // CQL1
        String CQL1 = String.format("select D_NEXT_O_ID from dbycql.District where D_W_ID = %d and D_ID = %d", W_ID, D_ID);
        simpleStatement = SimpleStatement.builder(CQL1)
                .setExecutionProfileName("oltp")
                .build();
        rs = cqlSession.execute(simpleStatement);
        int N = rs.one().getInt(0);

        // CQL2
        String CQL2 = String.format("select OL_I_ID from dbycql.OrderLine where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID >= %d - %d and OL_O_ID < %d allow filtering", W_ID, D_ID, N, L, N);
        Set<Integer> OL_I_IDs = new HashSet<>();
        simpleStatement = SimpleStatement.builder(CQL2)
                .setExecutionProfileName("oltp")
                .build();
        rs = cqlSession.execute(simpleStatement);

        rows = rs.all();
        for (Row row : rows) {
            int OL_I_ID = row.getInt(0);
            OL_I_IDs.add(OL_I_ID);
        }

        // CQL3
        int num = 0;
        for (int OL_I_ID : OL_I_IDs) {
            String CQL3 = String.format("select S_QUANTITY from dbycql.Stock where S_W_ID = %d and S_I_ID = %d allow filtering", W_ID, OL_I_ID);
            simpleStatement = SimpleStatement.builder(CQL3)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = cqlSession.execute(simpleStatement);
            BigDecimal S_QUANTITY = rs.one().getBigDecimal(0);
            double d = S_QUANTITY.doubleValue();
            if (d < T) num++;
        }
       logger.log(Level.FINE, "num=" + num);
    }

    @Override
    protected void YSQLExecute(Connection conn, Logger logger) throws SQLException {
        conn.setAutoCommit(false);
        try {
            java.sql.ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.StockLevelTransaction1.SQL, W_ID, D_ID));
            int D_NEXT_O_ID = -1;
            while (rs.next()) {
                D_NEXT_O_ID = rs.getInt(1);
               logger.log(Level.FINE, String.format("D_NEXT_O_ID=%d\n", D_NEXT_O_ID));
            }
            int N = D_NEXT_O_ID + 1;
            rs = conn.createStatement().executeQuery(String.format(SQLEnum.StockLevelTransaction2.SQL, W_ID, D_ID, N, L, N, T));
            while (rs.next()) {
                int cnt = rs.getInt(1);
               logger.log(Level.FINE, String.format("Count=%d\n", cnt));
            }

            conn.commit();
        } catch (SQLException e) {
            logger.log(Level.WARNING, "Error in STOCK Level transaction = ", e);
            if (conn != null) {
                logger.log(Level.WARNING, "Transaction is being rolled back");
                conn.rollback();
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    public int getW_ID() {
        return W_ID;
    }

    public void setW_ID(int w_ID) {
        W_ID = w_ID;
    }

    public int getD_ID() {
        return D_ID;
    }

    public void setD_ID(int d_ID) {
        D_ID = d_ID;
    }

    public int getT() {
        return T;
    }

    public void setT(int t) {
        T = t;
    }

    public int getL() {
        return L;
    }

    public void setL(int l) {
        L = l;
    }
}
