package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeliveryTransaction extends Transaction {
    int W_ID;
    int CARRIER_ID;

    @Override
    protected void YSQLExecute(Connection conn, Logger logger) throws SQLException {
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        try {
//            System.out.println("Delivery Transaction Begins..");
            stmt.execute(String.format("with min_order as(\n" +
                    "    select O_W_ID as MO_W_ID, O_D_ID as MO_D_ID, O_ID as MO_O_ID \n" +
                    "    from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID) as rank from Orders \n" +
                    "    where O_W_ID = %d and O_CARRIER_ID is null) t where rank = 1)\n" +
                    "update Orders set O_CARRIER_ID = %d from min_order t \n" +
                    "where O_W_ID = t.MO_W_ID and O_D_ID = t.MO_D_ID and O_ID = t.MO_O_ID", W_ID, CARRIER_ID));
            stmt.execute(String.format("with min_order as(\n" +
                    "    select O_W_ID as MO_W_ID, O_D_ID as MO_D_ID, O_ID as MO_O_ID, O_C_ID as MO_C_ID\n" +
                    "    from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID) as rank from Orders \n" +
                    "    where O_W_ID = %d and O_CARRIER_ID is null) t where rank = 1)\n" +
                    "update Orderline set OL_DELIVERY_D = (select current_timestamp) from min_order t \n" +
                    "where OL_W_ID = t.MO_W_ID and OL_D_ID = t.MO_D_ID and OL_O_ID = t.MO_O_ID", W_ID));
            stmt.execute(String.format("with min_order as(\n" +
                    "    select O_W_ID as MO_W_ID, O_D_ID as MO_D_ID, O_ID as MO_O_ID, O_C_ID as MO_C_ID\n" +
                    "    from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID) as rank from Orders \n" +
                    "    where O_W_ID = %d and O_CARRIER_ID is null) t where rank = 1),\n" +
                    "min_order_sum as(\n" +
                    "    select t1.MO_W_ID, t1.MO_D_ID, t1.MO_O_ID, MO_C_ID, sum(ol_amount) as B \n" +
                    "    from min_order t1 left join (select * from Orderline where OL_W_ID = %d) t2\n" +
                    "    on t1.MO_W_ID = t2.OL_W_ID and t1.MO_D_ID = t2.OL_D_ID and t1.MO_O_ID = t2.OL_O_ID \n" +
                    "    group by t1.MO_W_ID, t1.MO_D_ID, t1.MO_O_ID, t1.MO_C_ID)\n" +
                    "update Customer set C_BALANCE = C_BALANCE + t.B from min_order_sum t \n" +
                    "where C_W_ID = t.MO_W_ID and C_D_ID = t.MO_D_ID and C_ID = t.MO_C_ID", W_ID, W_ID));
            // 获取到每个district对应的最小order_number
//            for (int i = 1; i <= 1; i++) {
//                Statement stmt = conn.createStatement();
//                Statement stmt1 = conn.createStatement();
//                int[] tmpList = new int[2];
//                float[] tmpList1 = new float[1];
//                String sql = String.format("select O_ID, O_C_ID from Orders where O_W_ID = %d and O_D_ID = %d and O_CARRIER_ID is null order by O_ID limit 1", W_ID, i);
//                ResultSet rs = stmt.executeQuery(sql);
//                System.out.println(sql);
//                if (rs.next()) {
//                    tmpList[0] = rs.getInt(1);
//                    System.out.println(tmpList[0]);
//                    tmpList[1] = rs.getInt(2);
//                    System.out.println(tmpList[1]);
//                }
//                String sql2 = String.format("select sum(OL_AMOUNT) as B from Orderline where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", W_ID, i, tmpList[0]);
//                ResultSet rs1 = stmt1.executeQuery(sql2);
//                System.out.println(sql2);
//                if (rs1.next()) {
//                    tmpList1[0] = rs1.getFloat(1);
//                    System.out.println(tmpList1[0]);
//                }
//                stmt.executeUpdate(String.format("update Orders set O_CARRIER_ID = %d where O_W_ID = %d and O_D_ID = %d and O_ID = %d", CARRIER_ID, W_ID, i, tmpList[0]));
//                stmt.executeUpdate(String.format("update Orderline set OL_DELIVERY_D = (SELECT CURRENT_TIMESTAMP) where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", W_ID, i, tmpList[0]));
//                stmt.executeUpdate(String.format("update Customer SET C_BALANCE = C_BALANCE + %f, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 where C_W_ID = %d and C_D_ID = %d and C_ID = %d", tmpList1[0], W_ID, i, tmpList[1]));
//            ResultSet rs = stmt.executeQuery(String.format("select " +
//                    "t1.O_W_ID, t1.O_D_ID, t1.O_ID, t1.O_C_ID, t2.SUM_AMT " +
//                    "from(" +
//                    "select " +
//                    "O_W_ID, O_D_ID, O_ID, O_C_ID," +
//                    "rank() over(partition by O_W_ID,O_D_ID order by O_ID ASC) as rnk " +
//                    "FROM Orders WHERE O_W_ID=%d and O_CARRIER_ID is null" +
//                    ")t1 " +
//                    "left join " +
//                    "(SELECT " +
//                    "OL_W_ID, OL_D_ID, OL_O_ID," +
//                    "SUM(OL_AMOUNT) AS SUM_AMT " +
//                    "FROM " +
//                    "orderline " +
//                    "GROUP BY " +
//                    "OL_W_ID, OL_D_ID, OL_O_ID" +
//                    ")t2 " +
//                    "on t1.O_W_ID=t2.OL_W_ID AND t1.O_D_ID=t2.OL_D_ID AND t1.O_ID=t2.OL_O_ID " +
//                    "where t1.rnk=1", W_ID));
//            int[][] tmpList = new int[10][5];
//            int index = 0;
//            while (rs.next()) {
////               logger.log(Level.INFO, "存参数中...");
//                tmpList[index][0] = rs.getInt(1);
//                tmpList[index][1] = rs.getInt(2);
//                tmpList[index][2] = rs.getInt(3);
//                tmpList[index][3] = rs.getInt(4);
//                tmpList[index][4] = rs.getInt(5);
//                index++;
//            }
//           logger.log(Level.INFO, "Delivery Transaction正在执行中...");
//            // 写一个for循环
//            for (int i = 0; i < 10; i++) {
//                stmt.execute((String.format("UPDATE Orders SET O_CARRIER_ID=%d WHERE O_W_ID=%d and O_D_ID=%d and O_ID=%d", CARRIER_ID, tmpList[i][0], tmpList[i][1], tmpList[i][2])));
//                stmt.execute(String.format("UPDATE OrderLine SET OL_DELIVERY_D=(SELECT CURRENT_TIMESTAMP) WHERE OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d", tmpList[i][0], tmpList[i][1], tmpList[i][2]));
//                stmt.execute(String.format("UPDATE Customer SET C_BALANCE=C_BALANCE+%d WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", tmpList[i][3], tmpList[i][0], tmpList[i][1], tmpList[i][2]));
//                stmt.execute(String.format("UPDATE Customer SET C_DELIVERY_CNT=C_DELIVERY_CNT+%d WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", 1, tmpList[i][0], tmpList[i][1], tmpList[i][2]));
//            }
            conn.commit();
            logger.log(Level.INFO, "D transaction ends");
        } catch (SQLException e) {
            logger.log(Level.SEVERE, String.format("Error in %s transaction, exception= ",getTransactionType().type),e);
            e.printStackTrace();
            if (conn != null) {
                logger.log(Level.WARNING, "Transaction is being rolled back");
                conn.rollback();
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }


    protected void YCQLExecute(CqlSession session, Logger logger) {
        com.datastax.oss.driver.api.core.cql.ResultSet rs = null;
        SimpleStatement simpleStatement_0 = null;

        int o_ID = 0;
        int c_ID = 0;
        int max_Order = 0;
        float sum_Amt = 0;
        for (int d_ID = 1;  d_ID <= 10; d_ID ++) {
            // 第一个cql
            String CQL1 = String.format("select " +
                    "O_ID," +
                    "O_C_ID " +
                    "from dbycql.orders " +
                    "where O_W_ID=%d and O_CARRIER_ID = null and O_D_ID=%d " +
                    "LIMIT 1", W_ID, d_ID);
            simpleStatement_0 = SimpleStatement.builder(CQL1)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = session.execute(simpleStatement_0);

            Iterator<Row> rsIterator = rs.iterator();
            while (rsIterator.hasNext()) {
                Row row = rsIterator.next();
                o_ID = row.getInt(0);
                c_ID = row.getInt(1);
            }
            // 增加连接时间
            String first_cql = String.format("UPDATE dbycql.Orders SET O_CARRIER_ID=%d " +
                    "WHERE O_W_ID=%d and O_D_ID=%d and O_ID=%d", CARRIER_ID, W_ID, d_ID, o_ID);
            simpleStatement_0 = SimpleStatement.builder(first_cql)
                    .setExecutionProfileName("oltp")
                    .build();
            session.execute(simpleStatement_0);

            // 第二个cql
            String second_cql = String.format("select max(OL_NUMBER) as max_order " +
                    "from dbycql.OrderLine " +
                    "where OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d", W_ID, d_ID, o_ID);
            SimpleStatement simpleStatement0 = SimpleStatement.builder(second_cql)
                    .setExecutionProfileName("oltp")
                    .build();
            session.execute(simpleStatement0);
            rsIterator = rs.iterator();
            while (rsIterator.hasNext()) {
                Row row = rsIterator.next();
                max_Order = row.getInt(0);
            }

            // 第三个cql
            for (int ol_num = 1; ol_num < max_Order; ol_num++) {
                String CQL3 = String.format("SELECT OL_QUANTITY from dbycql.OrderLine WHERE OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d and OL_NUMBER=%d",  W_ID, d_ID, o_ID, ol_num);
                simpleStatement_0 = SimpleStatement.builder(CQL3)
                        .setExecutionProfileName("oltp")
                        .build();
                rs = session.execute(simpleStatement_0);

                rsIterator = rs.iterator();
                while (rsIterator.hasNext()) {
                    Row row = rsIterator.next();
                    float OL_QUANTITY = Objects.requireNonNull(row.getBigDecimal("OL_QUANTITY")).floatValue();
                    String third_cql = String.format("UPDATE dbycql.OrderLine SET OL_DELIVERY_D=toTimestamp(now()) " +
                            "WHERE OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d and OL_NUMBER=%d and OL_QUANTITY=%f", W_ID, d_ID, o_ID, ol_num, OL_QUANTITY);
                    SimpleStatement simpleStatement = SimpleStatement.builder(third_cql)
                            .setExecutionProfileName("oltp")
                            .build();
                    session.execute(simpleStatement);
                }
            }
            // 第四个cql
            String CQL4 = String.format("SELECT " +
                    "SUM(OL_AMOUNT) AS SUM_AMT " +
                    "FROM dbycql.OrderLine " +
                    "WHERE OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d " +
                    "allow filtering", W_ID, d_ID, o_ID);
            simpleStatement_0 = SimpleStatement.builder(CQL4)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = session.execute(simpleStatement_0);

            rsIterator = rs.iterator();
            while (rsIterator.hasNext()) {
                Row row = rsIterator.next();
                sum_Amt = Objects.requireNonNull(row.getBigDecimal("SUM_AMT")).floatValue();
            }
            // 第五个cql, 在外面更新C_BALANCE
            String CQL5 = String.format("SELECT C_BALANCE from dbycql.Customer WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", W_ID, d_ID, c_ID);
//            rs = session.execute(stmt);
            simpleStatement_0 = SimpleStatement.builder(CQL5)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = session.execute(simpleStatement_0);

            Iterator<Row> rs1Iterator = rs.iterator();
            while (rs1Iterator.hasNext()) {
                Row row = rs1Iterator.next();
                float tmp_balance = Objects.requireNonNull(row.getBigDecimal("C_BALANCE")).floatValue();
                // 传所有clustering key，删除符合条件的行
                tmp_balance += sum_Amt;
                SimpleStatement stmt2 = SimpleStatement.newInstance(String.format("SELECT * from dbycql.Customer WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", W_ID, d_ID, c_ID));
                com.datastax.oss.driver.api.core.cql.ResultSet rs2 = session.execute(stmt2);
                // 删除改行
                String CQL6 = String.format("DELETE from dbycql.Customer WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", W_ID, d_ID, c_ID);
                simpleStatement_0 = SimpleStatement.builder(CQL6)
                        .setExecutionProfileName("oltp")
                        .build();
                session.execute(simpleStatement_0);

                Iterator<Row> rs2Iterator = rs2.iterator();
                while (rs2Iterator.hasNext()) {
                    Row row1 = rs2Iterator.next();
                    Instant since = row1.getInstant("C_since");
                    if (since == null) {
                        String CQL7 = String.format("insert into dbycql.customer (C_W_id,C_D_id,C_id,C_first,C_middle,C_last,C_street_1,C_street_2,C_city,C_state,C_zip,C_phone,C_since,C_credit,C_credit_lim,C_discount,C_balance,C_ytd_payment,C_payment_cnt,C_delivery_cnt) " +
                                        "values (%d,%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',NULL,\'%s\',%f,%f,%f,%f,%d,%d)", W_ID, d_ID, c_ID, row1.getString("C_first"), row1.getString("C_middle"), row1.getString("C_last"), row1.getString("C_street_1"), row1.getString("C_street_2"),
                                row1.getString("C_city"), row1.getString("C_state"), row1.getString("C_zip"), row1.getString("C_phone"),  row1.getString("C_credit"), Objects.requireNonNull(row1.getBigDecimal("C_credit_lim")).floatValue(),
                                Objects.requireNonNull(row1.getBigDecimal("C_discount")).floatValue(), tmp_balance,
                                row1.getFloat("C_ytd_payment"), row1.getInt("C_payment_cnt"), row1.getInt("C_delivery_cnt")+1);
                        simpleStatement_0 = SimpleStatement.builder(CQL7)
                                .setExecutionProfileName("oltp")
                                .build();
                        rs = session.execute(simpleStatement_0);

                    }else {
                        String CQL8 = String.format("insert into dbycql.customer (C_W_id,C_D_id,C_id,C_first,C_middle,C_last,C_street_1,C_street_2,C_city,C_state,C_zip,C_phone,C_since,C_credit,C_credit_lim,C_discount,C_balance,C_ytd_payment,C_payment_cnt,C_delivery_cnt) " +
                                        "values (%d,%d,%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%f,%f,%f,%f,%d,%d)", W_ID, d_ID, c_ID, row1.getString("C_first"), row1.getString("C_middle"), row1.getString("C_last"), row1.getString("C_street_1"), row1.getString("C_street_2"),
                                row1.getString("C_city"), row1.getString("C_state"), row1.getString("C_zip"), row1.getString("C_phone"), since, row1.getString("C_credit"), Objects.requireNonNull(row1.getBigDecimal("C_credit_lim")).floatValue(),
                                Objects.requireNonNull(row1.getBigDecimal("C_discount")).floatValue(), tmp_balance,
                                row1.getFloat("C_ytd_payment"), row1.getInt("C_payment_cnt"), row1.getInt("C_delivery_cnt")+1);
                        simpleStatement_0 = SimpleStatement.builder(CQL8)
                                .setExecutionProfileName("oltp")
                                .build();
                        session.execute(simpleStatement_0);
                    }
                }
            }
        }
    }

    public int getW_ID() {
        return W_ID;
    }

    public void setW_ID(int w_ID) {
        W_ID = w_ID;
    }

    public int getCARRIER_ID() {
        return CARRIER_ID;
    }

    public void setCARRIER_ID(int carrier_ID) {
        CARRIER_ID = carrier_ID;
    }
}
