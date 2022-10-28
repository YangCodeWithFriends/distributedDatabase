import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;
import common.TransactionType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 5:28 PM
 */
public class ExecuteManager {
    private Set<TransactionType> skipSet;
    private List<Statistics> transactionTypeList;

    public ExecuteManager() {
        transactionTypeList = new ArrayList<>(8);
        skipSet = new HashSet<>(8);
        System.out.println(TransactionType.values());
        transactionTypeList.add(new Statistics(TransactionType.NEW_ORDER));
        transactionTypeList.add(new Statistics(TransactionType.PAYMENT));
        transactionTypeList.add(new Statistics(TransactionType.DELIVERY));
        transactionTypeList.add(new Statistics(TransactionType.ORDER_STATUS));
        transactionTypeList.add(new Statistics(TransactionType.STOCK_LEVEL));
        transactionTypeList.add(new Statistics(TransactionType.POPULAR_ITEM));
        transactionTypeList.add(new Statistics(TransactionType.TOP_BALANCE));
        transactionTypeList.add(new Statistics(TransactionType.RELATED_CUSTOMER));

        skipSet.add(TransactionType.NEW_ORDER);
//        skipSet.add(TransactionType.DELIVERY);
//        skipSet.add(TransactionType.RELATED_CUSTOMER);
    }

    public void executeYSQL(Connection conn, List<Transaction> list) throws SQLException {
        System.out.printf("Execute YSQL transactions\n");
        for (Transaction transaction : list) {
            if (skipSet.contains(transaction.getTransactionType())) continue;
            long executionTime = transaction.executeYSQL(conn);
            transactionTypeList.get(transaction.getTransactionType().index).addNewData(executionTime);
            report();
        }
    }

    public void executeYCQL(CqlSession session, List<Transaction> list) {
        System.out.printf("Execute YCQL transactions\n");
        for (Transaction transaction : list) {
            if (skipSet.contains(transaction.getTransactionType())) continue;
            long executionTime = transaction.executeYCQL(session);
            transactionTypeList.get(transaction.getTransactionType().index).addNewData(executionTime);
            report();
        }
    }

    public void report() {
        System.out.println("---Statistics start---");
        for (Statistics statistics : transactionTypeList) {
            System.out.println(statistics);
        }
        System.out.println("---Statistics end---");
    }

    public void reportCSV(Connection conn, CqlSession session) {
        float sum_w_ytd = 0;
        float sum_d_ytd = 0;
        int d_next_o_id = 0;
        float c_balance = 0;
        float c_ytd_payment = 0;
        int c_payment_cnt = 0;
        int c_delivery_cnt = 0;
        int o_id = 0;
        int o_ol_cnt = 0;
        float ol_amount = 0;
        float ol_quantity = 0;
        float s_quantity = 0;
        float s_ytd = 0;
        int s_order_cnt = 0;
        int s_remote_cnt = 0;
        try {
            // get all the sql information into the result set
            Statement stmt = conn.createStatement();
            ResultSet rs1 = stmt.executeQuery(String.format("select sum(W_YTD) from Warehouse"));
            ResultSet rs2 = stmt.executeQuery(String.format("select sum(D_YTD), sum(D_NEXT_O_ID) from District"));
            ResultSet rs3 = stmt.executeQuery(String.format("select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT)from Customer"));
            ResultSet rs4 = stmt.executeQuery(String.format("select max(O_ID), sum(O_OL_CNT) from Orders"));
            ResultSet rs5 = stmt.executeQuery(String.format("select sum(OL_AMOUNT), sum(OL_QUANTITY) from OrderLine"));
            ResultSet rs6 = stmt.executeQuery(String.format("select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock"));
            // get all the cql information into the result set
            // cql1
            SimpleStatement sim_stmt = SimpleStatement.newInstance(String.format("select W_YTD from dbycql.Warehouse"));
            com.datastax.oss.driver.api.core.cql.ResultSet cs1 = session.execute(sim_stmt);
            Iterator<Row> rsIterator1 = cs1.iterator();
            while (rsIterator1.hasNext()) {
                Row row = rsIterator1.next();
                sum_w_ytd += row.getFloat(0);
            }
            // cql2
            sim_stmt = SimpleStatement.newInstance(String.format("select D_YTD, D_NEXT_O_ID from dbycql.District"));
            com.datastax.oss.driver.api.core.cql.ResultSet cs2 = session.execute(sim_stmt);
            Iterator<Row> rsIterator2 = cs2.iterator();
            while (rsIterator2.hasNext()) {
                Row row = rsIterator2.next();
                sum_d_ytd += row.getFloat(0);
                d_next_o_id += row.getInt(1);
            }
            // cql3
            sim_stmt = SimpleStatement.newInstance(String.format("select C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT from dbycql.Customer"));
            com.datastax.oss.driver.api.core.cql.ResultSet cs3 = session.execute(sim_stmt);
            Iterator<Row> rsIterator3 = cs3.iterator();
            while (rsIterator3.hasNext()) {
                Row row = rsIterator3.next();
                c_balance += Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
                c_ytd_payment += row.getFloat(1);
                c_payment_cnt += row.getInt(2);
                c_delivery_cnt += row.getInt(3);
            }
            // cql4
            sim_stmt = SimpleStatement.newInstance(String.format("select O_ID, O_OL_CNT from dbycql.Orders"));
            com.datastax.oss.driver.api.core.cql.ResultSet cs4 = session.execute(sim_stmt);
            Iterator<Row> rsIterator4 = cs4.iterator();
            while (rsIterator4.hasNext()) {
                Row row = rsIterator4.next();
                o_id = Math.max(o_id, row.getInt(0));
                o_ol_cnt += row.getInt(1);
            }
            // cql5
            sim_stmt = SimpleStatement.newInstance(String.format("select OL_AMOUNT, OL_QUANTITY from dbycql.OrderLine"));
            com.datastax.oss.driver.api.core.cql.ResultSet cs5 = session.execute(sim_stmt);
            Iterator<Row> rsIterator5 = cs5.iterator();
            while (rsIterator5.hasNext()) {
                Row row = rsIterator5.next();
                ol_amount += Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
                ol_quantity += Objects.requireNonNull(row.getBigDecimal(1)).floatValue();
            }
            // cql6
            sim_stmt = SimpleStatement.newInstance(String.format("select S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT from dbycql.Stock"));
            com.datastax.oss.driver.api.core.cql.ResultSet cs6 = session.execute(sim_stmt);
            Iterator<Row> rsIterator6 = cs6.iterator();
            while (rsIterator6.hasNext()) {
                Row row = rsIterator6.next();
                s_quantity += Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
                s_ytd += Objects.requireNonNull(row.getBigDecimal(1)).floatValue();
                s_order_cnt += row.getInt(2);
                s_remote_cnt += row.getInt(3);
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
