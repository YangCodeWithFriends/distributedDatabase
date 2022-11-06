package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

// YSQL: 400ms, YCQL: 350ms
public class PaymentTransaction extends Transaction {

    int C_W_ID;
    int C_D_ID;
    int C_ID;
    float PAYMENT;

    protected void YSQLExecute(Connection conn, Logger logger) throws SQLException {
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(String.format("UPDATE Warehouse SET W_YTD = W_YTD + %f WHERE W_ID = %d RETURNING W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP", PAYMENT, C_W_ID));
            stmt.execute(String.format("UPDATE District SET D_YTD = D_YTD + %f WHERE D_W_ID = %d AND D_ID = %d RETURNING D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP", PAYMENT, C_W_ID, C_D_ID));
            stmt.execute(String.format("UPDATE Customer SET C_BALANCE = C_BALANCE - %f, C_YTD_PAYMENT = C_YTD_PAYMENT + %f, C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE C_W_ID = %d " +
                                        "AND C_D_ID = %d AND C_ID = %d " +
                                        "RETURNING C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, " +
                                        "C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT,C_CREDIT_LIM, C_DISCOUNT, C_BALANCE", PAYMENT, PAYMENT, C_W_ID, C_D_ID, C_ID));
            // example
//            String SQL1 = "update District set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = ? and D_ID = ? returning D_NEXT_O_ID;";
//            statement = conn.prepareStatement(SQL1);
//            statement.setInt(1, W_ID);
//            statement.setInt(2, D_ID);
//            rs = statement.executeQuery();
            // example end
//           logger.log(Level.FINE, stmt.getResultSet().getString(0));
//            stmt.execute(String.format("UPDATE Customer SET C_YTD_PAYMENT = C_YTD_PAYMENT + %f WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d", PAYMENT, C_W_ID, C_D_ID, C_ID));
//            stmt.execute(String.format("UPDATE Customer SET C_PAYMENT_CNT = C_PAYMENT_CNT + %d WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d", 1, C_W_ID, C_D_ID, C_ID));
//            ResultSet rs = stmt.executeQuery(String.format("select " +
//                    "t1.C_W_ID,t1.C_D_ID,t1.C_ID, " +
//                    "t1.C_FIRST, t1.C_MIDDLE, t1.C_LAST, " +
//                    "t1.C_STREET_1, t1.C_STREET_2, t1.C_CITY, t1.C_STATE, t1.C_ZIP, " +
//                    "t1.C_PHONE, t1.C_SINCE, t1.C_CREDIT, " +
//                    "t1.C_CREDIT_LIM, t1.C_DISCOUNT, t1.C_BALANCE," +
//                    "t2.W_STREET_1, t2.W_STREET_2, t2.W_CITY, t2.W_STATE, t2.W_ZIP, " +
//                    "t3.D_STREET_1, t3.D_STREET_2, t3.D_CITY, t3.D_STATE, t3.D_ZIP, " +
//                    "%f " +
//                    "FROM Customer t1 " +
//                    "left join " +
//                    "Warehouse t2 " +
//                    "on t1.C_W_ID=t2.W_ID " +
//                    "left join District t3 " +
//                    "on t1.C_D_ID=t3.D_ID " +
//                    "wHERE t1.C_W_ID=%d AND t1.C_D_ID=%d AND t1.C_ID=%d", PAYMENT, C_W_ID, C_D_ID, C_ID));
           logger.log(Level.FINE, "Payment Transaction正在执行中...");
//            while (rs.next()) {
//               logger.log(Level.FINE, "C_W_ID: " + rs.getInt(1) + "C_D_ID: " + rs.getInt(2) + "C_ID" + rs.getInt(3));
//               logger.log(Level.FINE, "C_FIRST: " + rs.getString(4) + "C_MIDDLE: " + rs.getString(5) + "C_LAST" + rs.getString(6));
//               logger.log(Level.FINE, "C_STREET_1: " + rs.getString(7) + "C_STREET_2: " + rs.getString(8) + "C_CITY: " + rs.getString(9) + "C_STATE: " + rs.getString(10) + "C_ZIP: " + rs.getString(11));
//               logger.log(Level.FINE, "C_PHONE：" + rs.getString(12) + "C_SINCE: " + rs.getString(13) + "C_CREDIT: " + rs.getString(14));
//               logger.log(Level.FINE, "C_CREDIT_LIM: " + rs.getFloat(15) + "C_DISCOUNT: " + rs.getFloat(16) + "C_BALANCE: " + rs.getFloat(17));
//               logger.log(Level.FINE, "W_STREET_1: " + rs.getString(18) + "W_STREET_2: " + rs.getString(19) + "W_CITY: " + rs.getString(20) + "W_STATE: " + rs.getString(21) + "W_ZIP: " + rs.getString(22));
//               logger.log(Level.FINE, "D_STREET_1: " + rs.getString(23) + "D_STREET_2: " + rs.getString(24) + "D_CITY: " + rs.getString(25) + "D_STATE: " + rs.getString(26) + "D_ZIP: " + rs.getString(27));
//            }
           logger.log(Level.FINE, "Payment Transaction执行完毕！");
           conn.commit();
        } catch (SQLException e) {
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

    protected void YCQLExecute(CqlSession session, Logger logger) {
       logger.log(Level.FINE, "执行payment cql中..");
        // cql1
        SimpleStatement stmt = SimpleStatement.newInstance(String.format("select W_YTD from dbycql.Warehouse where W_ID=%d", C_W_ID));
        com.datastax.oss.driver.api.core.cql.ResultSet rs1 = session.execute(stmt);
        Iterator<Row> rs1Iterator = rs1.iterator();
        while (rs1Iterator.hasNext()) {
            Row row = rs1Iterator.next();
            float tmp_payment = Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
            tmp_payment += PAYMENT;
            stmt = SimpleStatement.newInstance(String.format("UPDATE dbycql.Warehouse SET W_YTD=%f WHERE W_ID=%d", tmp_payment, C_W_ID));
            session.execute(stmt);
        }
        // cql2
        stmt = SimpleStatement.newInstance(String.format("select D_YTD from dbycql.District where D_W_ID=%d AND D_ID=%d", C_W_ID, C_D_ID));
        com.datastax.oss.driver.api.core.cql.ResultSet rs2 = session.execute(stmt);
        Iterator<Row> rs2Iterator = rs2.iterator();
        while (rs2Iterator.hasNext()) {
            Row row = rs2Iterator.next();
            float tmp_payment = Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
            tmp_payment += PAYMENT;
            stmt = SimpleStatement.newInstance(String.format("UPDATE dbycql.District SET D_YTD=%f WHERE D_W_ID=%d AND D_ID=%d", tmp_payment, C_W_ID, C_D_ID));
            session.execute(stmt);
        }
        // cql3
        stmt = SimpleStatement.newInstance(String.format("select C_BALANCE, C_YTD_PAYMENT from dbycql.Customer where C_W_ID=%d AND C_D_ID=%d AND C_ID=%d", C_W_ID, C_D_ID, C_ID));
        com.datastax.oss.driver.api.core.cql.ResultSet rs3 = session.execute(stmt);
        Iterator<Row> rs3Iterator = rs3.iterator();
        while (rs3Iterator.hasNext()) {
            Row row = rs3Iterator.next();
            float tmp_payment = row.getBigDecimal(0).floatValue();
            float tmp_ytd_payment = row.getFloat(1);
            tmp_payment -= PAYMENT;
            tmp_ytd_payment += PAYMENT;
            // 拿Customer表的所有列内容
            stmt = SimpleStatement.newInstance(String.format("SELECT * from dbycql.Customer where C_W_ID=%d AND C_D_ID=%d AND C_ID=%d", C_W_ID, C_D_ID, C_ID));
            com.datastax.oss.driver.api.core.cql.ResultSet rs4 = session.execute(stmt);
            // 删除改行
            stmt = SimpleStatement.newInstance(String.format("DELETE from dbycql.Customer WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", C_W_ID, C_D_ID, C_ID));
            session.execute(stmt);
            Iterator<Row> rs4Iterator = rs4.iterator();
            while (rs4Iterator.hasNext()) {
                Row row1 = rs4Iterator.next();
                SimpleStatement stmt4 = SimpleStatement.newInstance(String.format("insert into dbycql.customer (C_W_id,C_D_id,C_id,C_first,C_middle,C_last,C_street_1,C_street_2,C_city,C_state,C_zip,C_phone,C_since,C_credit,C_credit_lim,C_discount,C_balance,C_ytd_payment,C_payment_cnt,C_delivery_cnt) " +
                                "values (%d,%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',NULL,\'%s\',%f,%f,%f,%f,%d,%d)", C_W_ID, C_D_ID, C_ID, row1.getString("C_first"), row1.getString("C_middle"), row1.getString("C_last"), row1.getString("C_street_1"), row1.getString("C_street_2"),
                        row1.getString("C_city"), row1.getString("C_state"), row1.getString("C_zip"), row1.getString("C_phone"), row1.getString("C_credit"), Objects.requireNonNull(row1.getBigDecimal("C_credit_lim")).floatValue(),
                        Objects.requireNonNull(row1.getBigDecimal("C_discount")).floatValue(), tmp_payment,
                        tmp_ytd_payment, row1.getInt("C_payment_cnt"), row1.getInt("C_delivery_cnt")+1));
                session.execute(stmt4);
            }
//            String fourth_cql = String.format("UPDATE dbycql.Customer SET C_BALANCE=%f, C_YTD_PAYMENT=%f, C_PAYMENT_CNT=C_PAYMENT_CNT+1 WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d", tmp_payment, tmp_ytd_payment, C_W_ID, C_D_ID, C_ID);
//            SimpleStatement simpleStatement = SimpleStatement.builder(fourth_cql)
//                    .setExecutionProfileName("oltp")
//                    .build();
//            session.execute(simpleStatement);
        }
    }

    public int getC_W_ID() {
        return C_W_ID;
    }

    public void setC_W_ID(int c_w_ID) {
        C_W_ID = c_w_ID;
    }

    public int getC_D_ID() {
        return C_D_ID;
    }

    public void setC_D_ID(int c_d_ID) {
        C_D_ID = c_d_ID;
    }

    public int getC_ID() {
        return C_ID;
    }

    public void setC_ID(int c_ID) {
        C_ID = c_ID;
    }

    public float get_PAYMENT() {
        return PAYMENT;
    }

    public void set_PAYMENT(float payment) {
        PAYMENT = payment;
    }

    @Override
    public String toString() {
        return "PaymentTransaction{" +
                "C_W_ID=" + C_W_ID +
                ", C_D_ID=" + C_D_ID +
                ", C_ID=" + C_ID +
                "}\n";
    }
}
