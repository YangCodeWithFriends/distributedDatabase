import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;
import common.TransactionType;
import common.transactionImpl.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

public class SampleApp {
    private Connection conn;
    private CqlSession cqlSession;
    private static final int numberOfThreads = 20;
    private static int countDownLatchTimeout = 8;
    // 用来存20个client各自的transaction throughput
    private static ArrayList<Double> throughput_list = new ArrayList<>(numberOfThreads);
    private static double min = Double.MAX_VALUE;
    private static double max = -1;
    private static double avg = 0;
    private static double sum = 0;

    public static void main(String[] args) {
        // Set mode
        String MODE = DataSource.YSQL;// by default, run YSQL
        if (args != null && args.length != 0 && args[0].equals(DataSource.YCQL)) MODE = DataSource.YCQL;

        // Config logger for the main thread
        Logger mainLogger = Logger.getLogger(Thread.currentThread().getName());
        try {
            FileHandler handler = new FileHandler("log-main-thread-" + MODE + ".txt");
            handler.setFormatter(new SimpleFormatter());
            mainLogger.addHandler(handler);
        } catch (IOException e) {
            e.printStackTrace();
        }
        mainLogger.setLevel(Level.WARNING);

        mainLogger.log(Level.SEVERE, "Number of Threads = " + numberOfThreads);
        mainLogger.log(Level.SEVERE, "Your mode = " + MODE);

        // config input and output file.
        String[] inputFileList = new String[numberOfThreads];
        String[] outputFileList = new String[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            inputFileList[i] = "src/main/resources/xact_files/" + i + ".txt";
            outputFileList[i] = MODE + "-log-" + i + ".txt";
            Logger logger = Logger.getLogger(outputFileList[i]);
            try {
                Handler handler = new FileHandler(outputFileList[i]);
                handler.setFormatter(new SimpleFormatter());
                logger.addHandler(handler);
                // SETLEVEL. Set the logger filtering level.

                logger.setLevel(Level.WARNING);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

        // 创建一个全局Array，然后存每一个线程池执行完的时间，后续在report中计算avg，max以及min时间

        for (int i = 0; i < numberOfThreads; i++) {
            String finalMODE = MODE;
            int threadID = i;
            cachedThreadPool.execute(() -> {
                Logger logger = Logger.getLogger(outputFileList[threadID]);
                try {
                    logger.log(Level.SEVERE, Thread.currentThread().getName() + " starts ");
                    // 每一个client执行都会在执行完成之后存一个throughput进arraylist
                    new SampleApp().doWork(finalMODE, inputFileList[threadID], logger, threadID);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, Thread.currentThread().getName() + " exception ");
                    logger.log(Level.SEVERE, "exception = ", e);
                    e.printStackTrace();
                } finally {
                    logger.log(Level.SEVERE, Thread.currentThread().getName() + " ends ");
                    countDownLatch.countDown();
                }
            });
        }

        mainLogger.log(Level.SEVERE,"Main thread waits");
        try {
            mainLogger.log(Level.INFO,"CountDownLatchTimeout = " + countDownLatchTimeout);
            countDownLatch.await(countDownLatchTimeout, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            mainLogger.log(Level.SEVERE,"Exception: await interrupted exception",e);
        } finally {
            mainLogger.log(Level.SEVERE,"countDownLatch: " + countDownLatch.toString());
        }

        mainLogger.log(Level.SEVERE,"Main thread ends");
        cachedThreadPool.shutdown();

        // 在线程池结束之后开始统计arraylist中的值,min, max, avg
        for (double i : throughput_list) {
            min = Math.min(min, i);
            max = Math.max(max, i);
            sum += i;
        }
        if (!throughput_list.isEmpty()) {
            avg = sum / throughput_list.size();
        }
        // 根据模式输出切换文件夹和输出文件名
        if (MODE.equals(DataSource.YSQL)) {
            Path path = Paths.get("/tmp/dataCSV");
            try {
                Files.createDirectory(path);
                File writeSQLFile = new File("/tmp/client_sql.csv");
                try {
                    BufferedWriter writeText = new BufferedWriter(new FileWriter(writeSQLFile));
                    writeText.newLine();
                    writeText.write(min + "," + avg + "," +  max);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            Path path = Paths.get("/tmp/dataCSV");
            try {
                Files.createDirectory(path);
                File writeSQLFile = new File("/tmp/dataCSV/client_cql.csv");
                try {
                    BufferedWriter writeText = new BufferedWriter(new FileWriter(writeSQLFile));
                    writeText.newLine();
                    writeText.write(min + "," + avg + "," +  max);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Write throughput into file
        mainLogger.log(Level.SEVERE, String.format("min=%.2f,avg=%.2f,max=%.2f\n",min,avg,max));
    }

    public void doWork(String MODE, String inputFileName, Logger logger, int threadID) {
        logger.log(Level.SEVERE, Thread.currentThread().getName() + "do work");

        // 1. Construct requests from files.
        List<Transaction> list = new ArrayList<>();
        try {
            readFile(inputFileName, list, logger);
        } catch (FileNotFoundException e) {
            logger.log(Level.SEVERE, Thread.currentThread().getName() + " read file error");
            e.printStackTrace();
        }
        if (list == null) throw new RuntimeException("Input list is null! Please check input files");

        // 2. Establish a DB connection
        try {
            if (MODE.equals(DataSource.YSQL)) {
                logger.log(Level.WARNING, "Connecting to DB. Your mode is YSQL.");
                conn = new DataSource(MODE, threadID, logger).getSQLConnection();
                conn.setTransactionIsolation(1);
                logger.log(Level.INFO, "Conn = "+ conn.getClientInfo());
//                logger.log(Level.INFO, "Isolation level=" + conn.getTransactionIsolation());
            } else {
                logger.log(Level.WARNING, "Connecting to DB. Your mode is YCQL.");
                cqlSession = new DataSource(MODE, threadID, logger).getCQLSession();
                logger.log(Level.INFO, "CQLSession = "+ cqlSession.getName());
            }
            logger.log(Level.WARNING, ">>>> Successfully connected to YugabyteDB.");
        } catch (SQLException e) {
            e.printStackTrace();
            logger.log(Level.SEVERE, "DB Connection exception= ",e);
        }

        // 3. execute and report
        ExecuteManager executeManager = new ExecuteManager();
        if (MODE.equals(DataSource.YSQL)) {
            try {
                // 执行SQL的语句
                executeManager.executeYSQL(conn, list, logger);

                // 这是一个client执行完所有的transaction之后最后做的report操作，所以1和2的操作都是在这里
                executeManager.summary(logger);
                // 输出总共执行的transaction数量
                long cnt = executeManager.getCnt();
                logger.log(Level.WARNING,String.format("Total num: %d\n", cnt));
                // 获取该client的总执行时间
                long sum = executeManager.getTimeSum();
                double sum_time = sum / 1000.0;
                logger.log(Level.WARNING,String.format("Time sum(s): %.2f\n", sum_time));
                // 输出throughput
                double throughput = cnt * 1.0 / (sum / 1000.0);
                throughput_list.add(throughput);

                logger.log(Level.WARNING,String.format("Transaction throughput: %.2f\n", throughput));
                // 获取该client的执行平均时间并输出
                double avg_time = sum * 1.0 / cnt;
                logger.log(Level.WARNING,String.format("Time average(ms): %.2f\n", avg_time));
                // 获取8个transaction执行总时间组成的arraylist,然后输出中位数
                //ArrayList<Long> time_lst = executeManager.getTime_lst();
                ArrayList<Long> N1 = executeManager.getPercentage_time_lst();
                Collections.sort(N1);
                long medium;
                if (N1.size() % 2 == 0) {
                    medium = (N1.get(N1.size()/2-1) + N1.get(N1.size()/2)) / 2;
                }else {
                    medium = N1.get(N1.size()/2);
                }
                logger.log(Level.WARNING,String.format("Medium latency(ms): %d\n", medium));
                // 输出95%
                int index1 = (int) (N1.size() * 0.95);
                double num1 = (double) N1.get(index1);
                logger.log(Level.WARNING,String.format("95 latency(ms): %.2f\n", num1));
                // 输出99%
                int index2 = (int) (N1.size() * 0.99);
                double num2 = (double) N1.get(index2);
                logger.log(Level.WARNING,String.format("95 latency(ms): %.2f\n", num2));

                // 创建文件夹和写文件
                Path path = Paths.get("/tmp/dataCSV");
                try {
                    Files.createDirectory(path);
                    // 如果存在同名则覆盖文件
                    File writeSQLFile = new File("/tmp/dataCSV/clients_sql.csv");
                    try {
                        BufferedWriter writeText = new BufferedWriter(new FileWriter(writeSQLFile));
                        writeText.newLine();
                        writeText.write(cnt + "," + sum_time + "," +  throughput+ "," + avg_time + "," + medium+ "," + num1+ "," + num2);
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (SQLException e) {
                e.printStackTrace();
                logger.log(Level.SEVERE, "YSQL Execute exception= ",e);
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } else {
            try {
                // 开始执行CQL的代码
                executeManager.executeYCQL(cqlSession, list, logger);
                // 这是一个client执行完所有的transaction之后最后做的report操作，所以1和2的操作都是在这里
                executeManager.summary(logger);
                // 输出总共执行的transaction数量
                long cnt = executeManager.getCnt();
                logger.log(Level.WARNING,String.format("Total num: %d\n", cnt));
                // 获取该client的总执行时间
                long sum = executeManager.getTimeSum();
                double sum_time = sum / 1000.0;
                logger.log(Level.WARNING,String.format("Time sum(s): %.2f\n", sum_time));
                // 输出throughput
                double throughput = cnt * 1.0 / (sum / 1000.0);
                throughput_list.add(throughput);

                logger.log(Level.WARNING,String.format("Transaction throughput: %.2f\n", throughput));
                // 获取该client的执行平均时间并输出
                double avg_time = sum * 1.0 / cnt;
                logger.log(Level.WARNING,String.format("Time average(ms): %.2f\n", avg_time));
                // 获取8个transaction执行总时间组成的arraylist,然后输出中位数
                //ArrayList<Long> time_lst = executeManager.getTime_lst();
                ArrayList<Long> N1 = executeManager.getPercentage_time_lst();
                Collections.sort(N1);
                long medium;
                if (N1.size() % 2 == 0) {
                    medium = (N1.get(N1.size()/2-1) + N1.get(N1.size()/2)) / 2;
                }else {
                    medium = N1.get(N1.size()/2);
                }
                logger.log(Level.WARNING,String.format("Medium latency(ms): %d\n", medium));
                // 输出95%
                int index1 = (int) (N1.size() * 0.95);
                double num1 = (double) N1.get(index1);
                logger.log(Level.WARNING,String.format("95 latency(ms): %.2f\n", num1));
                // 输出99%
                int index2 = (int) (N1.size() * 0.99);
                double num2 = (double) N1.get(index2);
                logger.log(Level.WARNING,String.format("95 latency(ms): %.2f\n", num2));

                // 创建文件夹和写文件
                Path path = Paths.get("/tmp/dataCSV");
                try {
                    Files.createDirectory(path);
                    File writeSQLFile = new File("/tmp/dataCSV/client_cql.csv");
                    try {
                        BufferedWriter writeText = new BufferedWriter(new FileWriter(writeSQLFile));
                        writeText.newLine();
                        writeText.write(cnt + "," + sum_time + "," +  throughput+ "," + avg_time + "," + medium+ "," + num1+ "," + num2);
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                }

            } finally {
                cqlSession.close();
            }
        }

        // 输出performance measurement
        executeManager.reportSQL(conn);
        executeManager.reportCQL(cqlSession);
    }


    private static List<Transaction> readFile(String fileName, List<Transaction> list, Logger logger) throws FileNotFoundException {
        logger.log(Level.WARNING, Thread.currentThread().getName() + " reads from file " + fileName);
        Scanner scanner = new Scanner(new File(fileName));
        while (scanner.hasNextLine()) {
            String[] firstLine = scanner.nextLine().split(",");
            String type = firstLine[0];
            Transaction transaction = null;
            if (type.equals(TransactionType.PAYMENT.type)) {
                transaction = assemblePaymentTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.DELIVERY.type)) {
                transaction = assembleDeliveryTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.NEW_ORDER.type)) {
                transaction = assembleNewOrderTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.ORDER_STATUS.type)) {
                transaction = assembleOrderStatusTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.STOCK_LEVEL.type)) {
                transaction = assembleStockLevelTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.POPULAR_ITEM.type)) {
                transaction = assemblePopularItemTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.TOP_BALANCE.type)) {
                transaction = assembleTopBalanceTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.RELATED_CUSTOMER.type)) {
                transaction = assembleRelatedCustomerTransaction(firstLine, scanner);
            }
            if (transaction != null) list.add(transaction);
        }
        logger.log(Level.WARNING, Thread.currentThread().getName() + " reads " + list.size() + " requests from " + fileName);
        return list;
    }

    private static Transaction assembleRelatedCustomerTransaction(String[] firstLine, Scanner scanner) {
        int C_W_ID = Integer.parseInt(firstLine[1]);
        int C_D_ID = Integer.parseInt(firstLine[2]);
        int C_ID = Integer.parseInt(firstLine[3]);
        RelatedCustomerTransaction relatedCustomerTransaction = new RelatedCustomerTransaction();
        relatedCustomerTransaction.setTransactionType(TransactionType.RELATED_CUSTOMER);
        relatedCustomerTransaction.setC_W_ID(C_W_ID);
        relatedCustomerTransaction.setC_D_ID(C_D_ID);
        relatedCustomerTransaction.setC_ID(C_ID);
        return relatedCustomerTransaction;
    }

    private static Transaction assembleTopBalanceTransaction(String[] firstLine, Scanner scanner) {
        TopBalanceTransaction topBalanceTransaction = new TopBalanceTransaction();
        topBalanceTransaction.setTransactionType(TransactionType.TOP_BALANCE);
//        logger.log(Level.INFO, "add a top balance item trans");
        return topBalanceTransaction;
    }

    private static Transaction assemblePopularItemTransaction(String[] firstLine, Scanner scanner) {
        int W_ID = Integer.parseInt(firstLine[1]);
        int D_ID = Integer.parseInt(firstLine[2]);
        int L = Integer.parseInt(firstLine[3]);
        PopularItemTransaction popularItemTransaction = new PopularItemTransaction(W_ID, D_ID, L);
        popularItemTransaction.setTransactionType(TransactionType.POPULAR_ITEM);
//        logger.log(Level.INFO, "add a popular item trans");
        return popularItemTransaction;
    }

    private static Transaction assembleStockLevelTransaction(String[] firstLine, Scanner scanner) {
        int W_ID = Integer.parseInt(firstLine[1]);
        int D_ID = Integer.parseInt(firstLine[2]);
        int T = Integer.parseInt(firstLine[3]);
        int L = Integer.parseInt(firstLine[4]);
        StockLevelTransaction stockLevelTransaction = new StockLevelTransaction(W_ID, D_ID, T, L);
        stockLevelTransaction.setTransactionType(TransactionType.STOCK_LEVEL);
//        logger.log(Level.INFO, "add a stock level trans");
        return stockLevelTransaction;
    }

    private static Transaction assembleOrderStatusTransaction(String[] firstLine, Scanner scanner) {
        int C_W_ID = Integer.parseInt(firstLine[1]);
        int C_D_ID = Integer.parseInt(firstLine[2]);
        int C_ID = Integer.parseInt(firstLine[3]);
        OrderStatusTransaction orderStatusTransaction = new OrderStatusTransaction(C_W_ID, C_D_ID, C_ID);
        orderStatusTransaction.setTransactionType(TransactionType.ORDER_STATUS);
//        logger.log(Level.INFO, "add a order status trans");
        return orderStatusTransaction;
    }

    /*
    New Order Transaction consists of M+1 lines, where M denote the number of items in the new order.
    The first line consists of five comma-separated values: N,C ID,W ID,D ID,M.
    Each of the M remaining lines specifies an item in the order and consists of three comma- separated values: OL I ID,OL SUPPLY W ID,OL QUANTITY.
     */
    private static Transaction assembleNewOrderTransaction(String[] firstLine, Scanner scanner) {
        NewOrderTransaction newOrderTransaction = new NewOrderTransaction();
        int C_ID = Integer.parseInt(firstLine[1]);
        int W_ID = Integer.parseInt(firstLine[2]);
        int D_ID = Integer.parseInt(firstLine[3]);
        int M = Integer.parseInt(firstLine[4]);
        List<Integer> items = new ArrayList<>();
        List<Integer> suppliers = new ArrayList<>();
        List<Integer> quanties = new ArrayList<>();
        for (int i = 0; i < M; i++) {
            String[] strs = scanner.nextLine().split(",");
            int OL_I_ID = Integer.parseInt(strs[0]);
            int OL_SUPPLY_W_ID = Integer.parseInt(strs[1]);
            int OL_QUANTITY = Integer.parseInt(strs[2]);
            items.add(OL_I_ID);
            suppliers.add(OL_SUPPLY_W_ID);
            quanties.add(OL_QUANTITY);
        }
        newOrderTransaction.setTransactionType(TransactionType.NEW_ORDER);
        newOrderTransaction.setC_ID(C_ID);
        newOrderTransaction.setD_ID(D_ID);
        newOrderTransaction.setW_ID(W_ID);
        newOrderTransaction.setItems(items);
        newOrderTransaction.setQuantities(quanties);
        newOrderTransaction.setSupplierWarehouses(suppliers);
        return newOrderTransaction;
    }

    private static Transaction assembleDeliveryTransaction(String[] firstLine, Scanner scanner) {
        DeliveryTransaction deliveryTransaction = new DeliveryTransaction();
        int W_ID = Integer.parseInt(firstLine[1]);
        int CARRIER_ID = Integer.parseInt(firstLine[2]);
        deliveryTransaction.setTransactionType(TransactionType.DELIVERY);
        deliveryTransaction.setW_ID(W_ID);
        deliveryTransaction.setCARRIER_ID(CARRIER_ID);
        return deliveryTransaction;
    }

    private static Transaction assemblePaymentTransaction(String[] firstLine, Scanner scanner) {
        PaymentTransaction paymentTransaction = new PaymentTransaction();
        int C_W_ID = Integer.parseInt(firstLine[1]);
        int C_D_ID = Integer.parseInt(firstLine[2]);
        int C_ID = Integer.parseInt(firstLine[3]);
        float PAYMENT = Float.parseFloat(firstLine[4]);
        paymentTransaction.setTransactionType(TransactionType.PAYMENT);
        paymentTransaction.setC_ID(C_ID);
        paymentTransaction.setC_D_ID(C_D_ID);
        paymentTransaction.setC_W_ID(C_W_ID);
        paymentTransaction.set_PAYMENT(PAYMENT);
        return paymentTransaction;
    }
}
