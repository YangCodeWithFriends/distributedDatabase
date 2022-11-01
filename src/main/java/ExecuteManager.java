import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;
import common.TransactionType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 5:28 PM
 */
public class ExecuteManager {
    private Set<TransactionType> skipSet;
    private List<Statistics> transactionTypeList;
    private Map<TransactionType, Integer> skipMap;
    private int counter;
    private int LIMIT;
    // 定义变量
    private ArrayList<Long> time_lst = new ArrayList<>();
    private ArrayList<Long> percentage_time_lst = new ArrayList<Long>();
    private long sum = 0;
    private long cnt = 0;

    // 定义performance3变量
    private float sum_w_ytd = 0;
    private float sum_d_ytd = 0;
    private int sum_d_next_o_id = 0;
    private float sum_c_balance = 0;
    private float sum_c_ytd_payment = 0;
    private int sum_c_payment_cnt = 0;
    private int sum_c_delivery_cnt = 0;
    private int max_o_id = 0;
    private float sum_o_ol_cnt = 0;
    private float sum_ol_amount = 0;
    private float sum_ol_quantity = 0;
    private float sum_s_quantity = 0;
    private float sum_s_ytd = 0;
    private int sum_s_order_cnt = 0;
    private int sum_s_remote_cnt = 0;

    public ExecuteManager() {
        transactionTypeList = new ArrayList<>(8);
        skipSet = new HashSet<>(8);
        skipMap = new HashMap<>(8);
        counter = 0;

        transactionTypeList.add(new Statistics(TransactionType.NEW_ORDER));
        transactionTypeList.add(new Statistics(TransactionType.PAYMENT));
        transactionTypeList.add(new Statistics(TransactionType.DELIVERY));
        transactionTypeList.add(new Statistics(TransactionType.ORDER_STATUS));
        transactionTypeList.add(new Statistics(TransactionType.STOCK_LEVEL));
        transactionTypeList.add(new Statistics(TransactionType.POPULAR_ITEM));
        transactionTypeList.add(new Statistics(TransactionType.TOP_BALANCE));
        transactionTypeList.add(new Statistics(TransactionType.RELATED_CUSTOMER));

        for (TransactionType transactionType : TransactionType.values()) {
            skipMap.put(transactionType, 0);
        }
        LIMIT = 1;

//        LIMIT = 1;

        // 正选逻辑
//        skipSet.add(TransactionType.NEW_ORDER);
//        skipSet.add(TransactionType.DELIVERY);
//        skipSet.add(TransactionType.RELATED_CUSTOMER);

        // 反选逻辑
//        skipSet.addAll(Arrays.asList(TransactionType.values()));
//        skipSet.remove(TransactionType.POPULAR_ITEM);
    }

    public void executeYSQL(Connection conn, List<Transaction> list, Logger logger) throws SQLException {
        logger.log(Level.INFO, "Execute YSQL transactions\n");
        for (Transaction transaction : list) {
            if (skipSet.contains(transaction.getTransactionType())) continue;
            if (skipMap.containsKey(transaction.getTransactionType())) {
                int cnt = skipMap.getOrDefault(transaction.getTransactionType(), 0);
                if (cnt >= LIMIT) continue;
                skipMap.put(transaction.getTransactionType(), cnt + 1);
            }

            long executionTime = transaction.executeYSQL(conn, logger);
            transactionTypeList.get(transaction.getTransactionType().index).addNewData(executionTime);
            // 平常执行输出的是这个导致的
            report(logger);
        }
    }

    public void executeYCQL(CqlSession session, List<Transaction> list, Logger logger) {
        logger.log(Level.INFO, "Execute YCQL transactions\n");
        for (Transaction transaction : list) {
            if (skipSet.contains(transaction.getTransactionType())) continue;
            if (skipMap.containsKey(transaction.getTransactionType())) {
                int cnt = skipMap.getOrDefault(transaction.getTransactionType(), 0);
                if (cnt >= LIMIT) continue;
                skipMap.put(transaction.getTransactionType(), cnt + 1);
            }

            long executionTime = transaction.executeYCQL(session, logger);
            transactionTypeList.get(transaction.getTransactionType().index).addNewData(executionTime);
            // 平常执行输出的是这个导致的
            report(logger);
        }
    }

    public void report(Logger logger) {
        counter++; // print statistics every 5 transactions.
        // get all the transaction execution time and add them into list.
        for (Statistics statistics : transactionTypeList) {
            percentage_time_lst.add(statistics.getExeTime());
        }
        if (counter % 5 == 0) {
            logger.log(Level.INFO, "---Statistics start---");
            for (Statistics statistics : transactionTypeList) {
                logger.log(Level.INFO, statistics.toString());
            }
            logger.log(Level.INFO, "---Statistics end---");
        }
    }

    public void summary(Logger logger) {
        // 在最后一次输出的时候先格式化sum为0
        sum = 0;
        cnt = 0;
        time_lst = new ArrayList<Long>();
        for (Statistics statistics : transactionTypeList) {
            // 这是所有transaction的累计执行时间
//                time_lst.add(statistics.getTimeSum());
            sum += statistics.getTimeSum();
            // 获取到最后一次每个transaction执行的总时间
            time_lst.add(statistics.getTimeSum());
            // 获得执行的所有transaction的个数
            cnt += statistics.getCnt();
            logger.log(Level.SEVERE, statistics.toString());
        }
    }

    public long getCnt() {
        return cnt;
    }

    public long getTimeSum() {
        return sum;
    }

    public ArrayList<Long> getTime_lst() {
        return time_lst;
    }

    public ArrayList<Long> getPercentage_time_lst() {
        return percentage_time_lst;
    }

    public void reportSQL(Connection conn, Logger mainLogger) {
        try {
            // get all the sql information into the result set
            Statement stmt = conn.createStatement();
            ResultSet rs1 = stmt.executeQuery(String.format("select sum(W_YTD) from Warehouse"));
            if (rs1.next()) {
                sum_w_ytd = Objects.requireNonNull(rs1.getBigDecimal(1)).floatValue();
            }
            ResultSet rs2 = stmt.executeQuery(String.format("select sum(D_YTD), sum(D_NEXT_O_ID) from District"));
            if (rs2.next()) {
                sum_d_ytd = Objects.requireNonNull(rs2.getBigDecimal(1)).floatValue();
                sum_d_next_o_id = rs2.getInt(2);
            }
            ResultSet rs3 = stmt.executeQuery(String.format("select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT)from Customer"));
            if (rs3.next()) {
                sum_c_balance = Objects.requireNonNull(rs3.getBigDecimal(1)).floatValue();
                sum_c_ytd_payment = rs3.getFloat(2);
                sum_c_payment_cnt = rs3.getInt(3);
                sum_c_delivery_cnt = rs3.getInt(4);
            }
            ResultSet rs4 = stmt.executeQuery(String.format("select max(O_ID), sum(O_OL_CNT) from Orders"));
            if (rs4.next()) {
                max_o_id = rs4.getInt(1);
                sum_o_ol_cnt = rs4.getInt(2);
            }
            ResultSet rs5 = stmt.executeQuery(String.format("select sum(OL_AMOUNT), sum(OL_QUANTITY) from OrderLine"));
            if (rs5.next()) {
                sum_ol_amount = Objects.requireNonNull(rs5.getBigDecimal(1)).floatValue();
                sum_ol_quantity = Objects.requireNonNull(rs5.getBigDecimal(2)).floatValue();
            }
            ResultSet rs6 = stmt.executeQuery(String.format("select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from Stock"));
            if (rs6.next()) {
                sum_s_quantity = Objects.requireNonNull(rs6.getBigDecimal(1)).floatValue();
                sum_s_ytd = Objects.requireNonNull(rs6.getBigDecimal(2)).floatValue();
                sum_s_order_cnt = rs6.getInt(3);
                sum_s_remote_cnt = rs6.getInt(4);
            }
            // 拿完了所有的数据，开始进行输出到文件
//            Path path = Paths.get("/tmp/dataCSV");
            try {
//                Files.createDirectory(path);
                File writeSQLFile = new File("/tmp/dbstate_sql.csv");
                try {
                    BufferedWriter writeText = new BufferedWriter(new FileWriter(writeSQLFile));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_w_ytd));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_d_ytd));
                    writeText.newLine();
                    writeText.write(String.format("%d", sum_d_next_o_id));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_c_balance));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_c_ytd_payment));
                    writeText.newLine();
                    writeText.write(String.format("%d", sum_c_payment_cnt));
                    writeText.newLine();
                    writeText.write(String.format("%d", sum_c_delivery_cnt));
                    writeText.newLine();
                    writeText.write(String.format("%d", max_o_id));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_o_ol_cnt));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_ol_amount));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_ol_quantity));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_s_quantity));
                    writeText.newLine();
                    writeText.write(String.format("%f", sum_s_ytd));
                    writeText.newLine();
                    writeText.write(String.format("%d", sum_s_order_cnt));
                    writeText.newLine();
                    writeText.write(String.format("%d", sum_s_remote_cnt));

//                    writeText.write(sum_w_ytd + "," + sum_d_ytd + "," +  sum_d_next_o_id+ "," + sum_c_balance + "," + sum_c_ytd_payment+ "," + sum_c_payment_cnt+ "," + sum_c_delivery_cnt + "," + max_o_id + ","
//                    + sum_o_ol_cnt + "," + sum_ol_amount + "," + sum_ol_quantity + "," + sum_s_quantity + "," + sum_s_ytd + "," + sum_s_order_cnt + "," + sum_s_remote_cnt);
                    writeText.flush();
                    writeText.close();
                }catch (Exception e) {
                    e.printStackTrace();
                    mainLogger.log(Level.SEVERE, "reportSQL write file exception = ", e);
                }
            }catch (Exception e) {
                e.printStackTrace();
                mainLogger.log(Level.SEVERE, "reportSQL middle exception = ", e);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            mainLogger.log(Level.SEVERE, "reportSQL exception = ", e);
        }
    }

    public void reportCQL(CqlSession session, Logger mainLogger) {
        // get all the cql information into the result set
        SimpleStatement simpleStatement_0 = null;
        // cql1
//        System.out.println("开始执行cql1");
        String cql1 = String.format("select sum(W_YTD) from dbycql.Warehouse");
        simpleStatement_0 = SimpleStatement.builder(cql1)
                .setExecutionProfileName("oltp")
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet cs1 = session.execute(simpleStatement_0);
        Iterator<Row> rsIterator1 = cs1.iterator();
        if (rsIterator1.hasNext()) {
            Row row = rsIterator1.next();
            sum_w_ytd = Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
        }
//        System.out.println("结束执行cql1");
        // cql2
//        System.out.println("开始执行cql2");
        String cql2 = String.format("select sum(D_YTD), sum(D_NEXT_O_ID) from dbycql.District");
        simpleStatement_0 = SimpleStatement.builder(cql2)
                .setExecutionProfileName("oltp")
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet cs2 = session.execute(simpleStatement_0);
        Iterator<Row> rsIterator2 = cs2.iterator();
        if (rsIterator2.hasNext()) {
            Row row = rsIterator2.next();
            sum_d_ytd = Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
            sum_d_next_o_id = row.getInt(1);
        }
//        System.out.println("结束执行cql2");
        // cql3
//        System.out.println("开始执行cql3");
        String cql3 = String.format("select sum(C_BALANCE), sum(C_YTD_PAYMENT), sum(C_PAYMENT_CNT), sum(C_DELIVERY_CNT) from dbycql.Customer");
        simpleStatement_0 = SimpleStatement.builder(cql3)
                .setExecutionProfileName("oltp")
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet cs3 = session.execute(simpleStatement_0);
        Iterator<Row> rsIterator3 = cs3.iterator();
        if (rsIterator3.hasNext()) {
            Row row = rsIterator3.next();
            sum_c_balance = Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
            sum_c_ytd_payment = row.getFloat(1);
            sum_c_payment_cnt = row.getInt(2);
            sum_c_delivery_cnt = row.getInt(3);
        }
//        System.out.println("结束执行cql3");
        // cql4
//        System.out.println("开始执行cql4");
        String cql4 = String.format("select max(O_ID), sum(O_OL_CNT) from dbycql.Orders");
        simpleStatement_0 = SimpleStatement.builder(cql4)
                .setExecutionProfileName("oltp")
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet cs4 = session.execute(simpleStatement_0);
        Iterator<Row> rsIterator4 = cs4.iterator();
        if (rsIterator4.hasNext()) {
            Row row = rsIterator4.next();
            max_o_id = row.getInt(0);
//            max_o_id = Math.max(max_o_id, row.getInt(0));
            sum_o_ol_cnt = Objects.requireNonNull(row.getBigDecimal(1)).floatValue();
        }
//        System.out.println("结束执行cql4");
        // cql5
//        System.out.println("开始执行cql5");
        String cql5 = String.format("select sum(OL_AMOUNT), sum(OL_QUANTITY) from dbycql.OrderLine");
        simpleStatement_0 = SimpleStatement.builder(cql5)
                .setExecutionProfileName("oltp")
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet cs5 = session.execute(simpleStatement_0);
        Iterator<Row> rsIterator5 = cs5.iterator();
        if (rsIterator5.hasNext()) {
            Row row = rsIterator5.next();
            sum_ol_amount = Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
            sum_ol_quantity = Objects.requireNonNull(row.getBigDecimal(1)).floatValue();
        }
//        System.out.println("结束执行cql5");
        // cql6
//        System.out.println("开始执行cql6");
        String cql6 = String.format("select sum(S_QUANTITY), sum(S_YTD), sum(S_ORDER_CNT), sum(S_REMOTE_CNT) from dbycql.Stock");
        simpleStatement_0 = SimpleStatement.builder(cql6)
                .setExecutionProfileName("oltp")
                .build();
        com.datastax.oss.driver.api.core.cql.ResultSet cs6 = session.execute(simpleStatement_0);
        Iterator<Row> rsIterator6 = cs6.iterator();
        if (rsIterator6.hasNext()) {
            Row row = rsIterator6.next();
            sum_s_quantity = Objects.requireNonNull(row.getBigDecimal(0)).floatValue();
            sum_s_ytd = Objects.requireNonNull(row.getBigDecimal(1)).floatValue();
            sum_s_order_cnt = row.getInt(2);
            sum_s_remote_cnt = row.getInt(3);
        }
//        System.out.println("结束执行cql6");
        // 拿完了所有的数据，开始进行输出到文件
//        Path path = Paths.get("/tmp/dataCSV");
        try {
//            Files.createDirectory(path);
            File writeSQLFile = new File("/tmp/dbstate_cql.csv");
            try {
                BufferedWriter writeText = new BufferedWriter(new FileWriter(writeSQLFile));
                writeText.newLine();
                writeText.write(String.format("%f", sum_w_ytd));
                writeText.newLine();
                writeText.write(String.format("%f", sum_d_ytd));
                writeText.newLine();
                writeText.write(String.format("%d", sum_d_next_o_id));
                writeText.newLine();
                writeText.write(String.format("%f", sum_c_balance));
                writeText.newLine();
                writeText.write(String.format("%f", sum_c_ytd_payment));
                writeText.newLine();
                writeText.write(String.format("%d", sum_c_payment_cnt));
                writeText.newLine();
                writeText.write(String.format("%d", sum_c_delivery_cnt));
                writeText.newLine();
                writeText.write(String.format("%d", max_o_id));
                writeText.newLine();
                writeText.write(String.format("%f", sum_o_ol_cnt));
                writeText.newLine();
                writeText.write(String.format("%f", sum_ol_amount));
                writeText.newLine();
                writeText.write(String.format("%f", sum_ol_quantity));
                writeText.newLine();
                writeText.write(String.format("%f", sum_s_quantity));
                writeText.newLine();
                writeText.write(String.format("%f", sum_s_ytd));
                writeText.newLine();
                writeText.write(String.format("%d", sum_s_order_cnt));
                writeText.newLine();
                writeText.write(String.format("%d", sum_s_remote_cnt));

//                writeText.write(sum_w_ytd + "," + sum_d_ytd + "," +  sum_d_next_o_id+ "," + sum_c_balance + "," + sum_c_ytd_payment+ "," + sum_c_payment_cnt+ "," + sum_c_delivery_cnt + "," + max_o_id + ","
//                        + sum_o_ol_cnt + "," + sum_ol_amount + "," + sum_ol_quantity + "," + sum_s_quantity + "," + sum_s_ytd + "," + sum_s_order_cnt + "," + sum_s_remote_cnt);
                writeText.flush();
                writeText.close();
            }catch (Exception e) {
                e.printStackTrace();
                mainLogger.log(Level.SEVERE, "report write file exception = ", e);
            }
        }catch (Exception e) {
            e.printStackTrace();
            mainLogger.log(Level.SEVERE, "reportCQL exception = ", e);
        }
    }
}
