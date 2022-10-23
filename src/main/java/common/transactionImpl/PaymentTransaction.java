package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// YSQL: 400ms, YCQL: 350ms
public class PaymentTransaction extends Transaction {

    int C_W_ID;
    int C_D_ID;
    int C_ID;
    float PAYMENT;

    protected void YSQLExecute(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(String.format("UPDATE Warehouse SET W_YTD = W_YTD + %f WHERE W_ID = %d", PAYMENT, C_W_ID));
        stmt.execute(String.format("UPDATE District SET D_YTD = D_YTD + %f WHERE D_W_ID = %d AND D_ID = %d", PAYMENT, C_W_ID, C_D_ID));
        stmt.execute(String.format("UPDATE Customer SET C_BALANCE = C_BALANCE - %f WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d", PAYMENT, C_W_ID, C_D_ID, C_ID));
        stmt.execute(String.format("UPDATE Customer SET C_YTD_PAYMENT = C_YTD_PAYMENT + %f WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d", PAYMENT, C_W_ID, C_D_ID, C_ID));
        stmt.execute(String.format("UPDATE Customer SET C_PAYMENT_CNT = C_PAYMENT_CNT + %d WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d", 1, C_W_ID, C_D_ID, C_ID));
        ResultSet rs = stmt.executeQuery(String.format("select " +
                "t1.C_W_ID,t1.C_D_ID,t1.C_ID, " +
                "t1.C_FIRST, t1.C_MIDDLE, t1.C_LAST, " +
                "t1.C_STREET_1, t1.C_STREET_2, t1.C_CITY, t1.C_STATE, t1.C_ZIP, " +
                "t1.C_PHONE, t1.C_SINCE, t1.C_CREDIT, " +
                "t1.C_CREDIT_LIM, t1.C_DISCOUNT, t1.C_BALANCE," +
                "t2.W_STREET_1, t2.W_STREET_2, t2.W_CITY, t2.W_STATE, t2.W_ZIP, " +
                "t3.D_STREET_1, t3.D_STREET_2, t3.D_CITY, t3.D_STATE, t3.D_ZIP, " +
                "%f " +
                "FROM Customer t1 " +
                "left join " +
                "Warehouse t2 " +
                "on t1.C_W_ID=t2.W_ID " +
                "left join District t3 " +
                "on t1.C_D_ID=t3.D_ID " +
                "wHERE t1.C_W_ID=%d AND t1.C_D_ID=%d AND t1.C_ID=%d", PAYMENT, C_W_ID, C_D_ID, C_ID));
        System.out.println("Payment Transaction正在执行中...");
        while (rs.next()) {
            System.out.println("C_W_ID: " + rs.getInt(1) + "C_D_ID: " + rs.getInt(2) + "C_ID" + rs.getInt(3));
            System.out.println("C_FIRST: " + rs.getString(4) + "C_MIDDLE: " + rs.getString(5) + "C_LAST" + rs.getString(6));
            System.out.println("C_STREET_1: " + rs.getString(7) + "C_STREET_2: " + rs.getString(8) + "C_CITY: " + rs.getString(9) + "C_STATE: " + rs.getString(10) + "C_ZIP: " + rs.getString(11));
            System.out.println("C_PHONE：" + rs.getString(12) + "C_SINCE: " + rs.getString(13) + "C_CREDIT: " + rs.getString(14));
            System.out.println("C_CREDIT_LIM: " + rs.getFloat(15) + "C_DISCOUNT: " + rs.getFloat(16) + "C_BALANCE: " + rs.getFloat(17));
            System.out.println("W_STREET_1: " + rs.getString(18) + "W_STREET_2: " + rs.getString(19) + "W_CITY: " + rs.getString(20) + "W_STATE: " + rs.getString(21) + "W_ZIP: " + rs.getString(22));
            System.out.println("D_STREET_1: " + rs.getString(23) + "D_STREET_2: " + rs.getString(24) + "D_CITY: " + rs.getString(25) + "D_STATE: " + rs.getString(26) + "D_ZIP: " + rs.getString(27));
        }
        System.out.println("Payment Transaction执行完毕！");
    }

    protected void YCQLExecute(CqlSession session) {
        System.out.println("执行payment cql中..");
        SimpleStatement stmt = SimpleStatement.newInstance(String.format("UPDATE dbycql.Warehouse_counter SET W_YTD=W_YTD+%d WHERE W_ID=%d", (int) PAYMENT, C_W_ID));
        session.execute(stmt);
        stmt = SimpleStatement.newInstance(String.format("UPDATE dbycql.District_counter SET D_YTD=D_YTD+%d WHERE D_W_ID=%d AND D_ID=%d", (int) PAYMENT, C_W_ID, C_D_ID));
        session.execute(stmt);
        stmt = SimpleStatement.newInstance(String.format("UPDATE dbycql.Customer_counter SET C_BALANCE=C_BALANCE-%d WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d", (int) PAYMENT, C_W_ID, C_D_ID, C_ID));
        session.execute(stmt);
        stmt = SimpleStatement.newInstance(String.format("UPDATE dbycql.Customer_counter SET C_YTD_PAYMENT=C_YTD_PAYMENT+%d WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d", (int) PAYMENT, C_W_ID, C_D_ID, C_ID));
        session.execute(stmt);
        stmt = SimpleStatement.newInstance(String.format("UPDATE dbycql.Customer_counter SET C_PAYMENT_CNT=C_PAYMENT_CNT+%d WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d", 1, C_W_ID, C_D_ID, C_ID));
        session.execute(stmt);
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
