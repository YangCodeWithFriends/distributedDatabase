package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;

import java.sql.*;
import java.time.Instant;
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
            // Statement stmt = conn.createStatement();
            PreparedStatement statement = null;
            ResultSet rs = null;
            String SQL1 = "UPDATE Warehouse SET W_YTD = W_YTD + ? WHERE W_ID = ? RETURNING W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP;";
            statement = conn.prepareStatement(SQL1);
            statement.setFloat(1, PAYMENT);
            statement.setInt(2, C_W_ID);
            rs = statement.executeQuery();
            String W_STREET_1 = null, W_STREET_2 = null, W_CITY = null, W_STATE = null, W_ZIP = null;
            while (rs.next()) {
                W_STREET_1 = rs.getString(1);
                W_STREET_2 = rs.getString(2);
                W_CITY = rs.getString(3);
                W_STATE = rs.getString(4);
                W_ZIP = rs.getString(5);
            }

            String SQL2 = "UPDATE District SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ? RETURNING D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP;";
            statement = conn.prepareStatement(SQL2);
            statement.setFloat(1, PAYMENT);
            statement.setInt(2, C_W_ID);
            statement.setInt(3, C_D_ID);
            String D_STREET_1 = null, D_STREET_2 = null, D_CITY = null, D_STATE = null, D_ZIP = null;
            rs = statement.executeQuery();
            while (rs.next()) {
                D_STREET_1 = rs.getString(1);
                D_STREET_2 = rs.getString(2);
                D_CITY = rs.getString(3);
                D_STATE = rs.getString(4);
                D_ZIP = rs.getString(5);
            }

            String SQL3 = "UPDATE Customer SET C_BALANCE = C_BALANCE - ?, C_YTD_PAYMENT = C_YTD_PAYMENT + ?, C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE C_W_ID = ? " +
                    "AND C_D_ID = ? AND C_ID = ? " +
                    "RETURNING C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, " +
                    "C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT,C_CREDIT_LIM, C_DISCOUNT, C_BALANCE;";
            statement = conn.prepareStatement(SQL3);
            statement.setFloat(1, PAYMENT);
            statement.setFloat(2, PAYMENT);
            statement.setInt(3, C_W_ID);
            statement.setInt(4, C_D_ID);
            statement.setInt(5, C_ID);
            int c_w_id = 0, c_d_id = 0, c_id = 0;
            String C_FIRST = null, C_MIDDLE = null, C_LAST = null, C_STREET_1 = null, C_STREET_2 = null, C_CITY = null, C_STATE = null, C_ZIP = null, C_PHONE = null, C_CREDIT = null;
            String C_SINCE = null;
            float C_CREDIT_LIM = 0, C_DISCOUNT = 0, C_BALANCE  = 0;
            rs = statement.executeQuery();
            while (rs.next()) {
                c_w_id = rs.getInt(1);
                c_d_id = rs.getInt(2);
                c_id = rs.getInt(3);
                C_FIRST = rs.getString(4);
                C_MIDDLE = rs.getString(5);
                C_LAST = rs.getString(6);
                C_STREET_1 = rs.getString(7);
                C_STREET_2 = rs.getString(8);
                C_CITY = rs.getString(9);
                C_STATE = rs.getString(10);
                C_ZIP = rs.getString(11);
                C_PHONE = rs.getString(12);
                C_SINCE = rs.getTimestamp(13).toString();
                C_CREDIT = rs.getString(14);
                C_CREDIT_LIM = Objects.requireNonNull(rs.getBigDecimal(15)).floatValue();
                C_DISCOUNT = Objects.requireNonNull(rs.getBigDecimal(16)).floatValue();
                C_BALANCE = Objects.requireNonNull(rs.getBigDecimal(17)).floatValue();
            }
            logger.log(Level.INFO, String.format("W_STREET_1 = %s, W_STREET_2 = %s, W_CITY = %s, W_STATE = %s, W_ZIP = %s, " +
                    "D_STREET_1 = %s, D_STREET_2 = %s, D_CITY = %s, D_STATE = %s, D_ZIP = %s, " +
                    "c_w_id = %d, c_d_id = %d, c_id = %d, C_FIRST = %s, C_MIDDLE = %s, C_LAST = %s," +
                    "C_STREET_1 = %s, C_STREET_2 = %s, C_CITY = %s, C_STATE = %s, C_ZIP = %s, C_PHONE = %s, C_SINCE = %s," +
                    "C_CREDIT = %s, C_CREDIT_LIM = %f, C_DISCOUNT = %f, C_BALANCE = %f, PAYMENT = %f", W_STREET_1, W_STREET_2, W_CITY, W_STATE,
                    W_ZIP, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, c_w_id, c_d_id, c_id, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
                    C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, PAYMENT));
           conn.commit();
        } catch (SQLException e) {
            logger.log(Level.SEVERE, String.format("Error in %s transaction, exception= ",getTransactionType().type),e);
            if (conn != null) {
                logger.log(Level.WARNING, "Transaction is being rolled back");
                conn.rollback();
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }



    protected void YCQLExecute(CqlSession session, Logger logger) {
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
        }
        // cql4
        String W_STREET_1 = null, W_STREET_2 = null, W_CITY = null, W_STATE = null, W_ZIP = null;
        String D_STREET_1 = null, D_STREET_2 = null, D_CITY = null, D_STATE = null, D_ZIP = null;
        int c_w_id = 0, c_d_id = 0, c_id = 0;
        String C_FIRST = null, C_MIDDLE = null, C_LAST = null, C_STREET_1 = null, C_STREET_2 = null, C_CITY = null, C_STATE = null, C_ZIP = null, C_PHONE = null, C_CREDIT = null;
        Instant C_SINCE = null;
        float C_CREDIT_LIM = 0, C_DISCOUNT = 0, C_BALANCE  = 0;
        stmt = SimpleStatement.newInstance(String.format("select\n" +
                "            W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP\n" +
                "    from dbycql.warehouse\n" +
                "    where W_ID=%d", C_W_ID));
        com.datastax.oss.driver.api.core.cql.ResultSet rs5 = session.execute(stmt);
        Iterator<Row> rs5Iterator = rs5.iterator();
        if (rs5Iterator.hasNext()) {
            Row row = rs5Iterator.next();
            W_STREET_1 = row.getString(0);
            W_STREET_2 = row.getString(1);
            W_CITY = row.getString(2);
            W_STATE = row.getString(3);
            W_ZIP = row.getString(4);
        }
        stmt = SimpleStatement.newInstance(String.format("select\n" +
                "            C_W_ID,C_D_ID,C_ID,\n" +
                "            C_FIRST, C_MIDDLE, C_LAST,\n" +
                "            C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,\n" +
                "            C_PHONE, C_SINCE, C_CREDIT,\n" +
                "            C_CREDIT_LIM, C_DISCOUNT, C_BALANCE\n" +
                "    from dbycql.Customer\n" +
                "    where C_W_ID=%d AND C_D_ID=%d AND C_ID=%d", C_W_ID, C_D_ID, C_ID));
        com.datastax.oss.driver.api.core.cql.ResultSet rs6 = session.execute(stmt);
        Iterator<Row> rs6Iterator = rs6.iterator();
        if (rs6Iterator.hasNext()) {
            Row row = rs6Iterator.next();
            c_w_id = row.getInt(0);
            c_d_id = row.getInt(1);
            c_id = row.getInt(2);
            C_FIRST = row.getString(3);
            C_MIDDLE = row.getString(4);
            C_LAST = row.getString(5);
            C_STREET_1 = row.getString(6);
            C_STREET_2 = row.getString(7);
            C_CITY = row.getString(8);
            C_STATE = row.getString(9);
            C_ZIP = row.getString(10);
            C_PHONE = row.getString(11);
            C_SINCE = row.getInstant(12);
            C_CREDIT = row.getString(13);
            C_CREDIT_LIM = Objects.requireNonNull(row.getBigDecimal(14)).floatValue();
            C_DISCOUNT = Objects.requireNonNull(row.getBigDecimal(15)).floatValue();
            C_BALANCE = Objects.requireNonNull(row.getBigDecimal(16)).floatValue();
        }
        stmt = SimpleStatement.newInstance(String.format("    select\n" +
                "    D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from dbycql.district\n" +
                "    where D_W_ID=%d AND D_ID=%d", C_W_ID, C_D_ID));
        com.datastax.oss.driver.api.core.cql.ResultSet rs7 = session.execute(stmt);
        Iterator<Row> rs7Iterator = rs7.iterator();
        if (rs7Iterator.hasNext()) {
            Row row = rs7Iterator.next();
            D_STREET_1 = row.getString(0);
            D_STREET_2 = row.getString(1);
            D_CITY = row.getString(2);
            D_STATE = row.getString(3);
            D_ZIP = row.getString(4);
        }
        logger.log(Level.INFO, String.format("W_STREET_1 = %s, W_STREET_2 = %s, W_CITY = %s, W_STATE = %s, W_ZIP = %s, " +
                        "D_STREET_1 = %s, D_STREET_2 = %s, D_CITY = %s, D_STATE = %s, D_ZIP = %s, " +
                        "C_W_ID = %d, C_D_ID = %d, C_ID = %d, C_FIRST = %s, C_MIDDLE = %s, C_LAST = %s," +
                        "C_STREET_1 = %s, C_STREET_2 = %s, C_CITY = %s, C_STATE = %s, C_ZIP = %s, C_PHONE = %s, C_SINCE = %s," +
                        "C_CREDIT = %s, C_CREDIT_LIM = %f, C_DISCOUNT = %f, C_BALANCE = %f, PAYMENT = %f", W_STREET_1, W_STREET_2, W_CITY, W_STATE,
                W_ZIP, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, c_w_id, c_d_id, c_id, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,
                C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, PAYMENT));
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
