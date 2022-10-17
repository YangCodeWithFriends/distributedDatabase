package common;

import com.datastax.oss.driver.api.core.CqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @Package common
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 4:02 PM
 */
public abstract class Transaction {
    // define template method for transaction execution
    TransactionType transactionType;
    long startTimeStamp;

    public void executeYCQL(CqlSession cqlSession) {
        beforeExecute();
        execute(cqlSession);
        postExecute();
    }

    protected void beforeExecute() {
        startTimeStamp = System.currentTimeMillis();
        System.out.printf(transactionType.type + " Transaction begins\n");
    }

    protected void execute(CqlSession cqlSession) {

    }

    protected void postExecute() {
        long endTimeStamp = System.currentTimeMillis();
        long seconds = TimeUnit.MILLISECONDS.toMillis(endTimeStamp - startTimeStamp);
        System.out.printf("%s completes,takes %d milliseconds\n",transactionType, seconds);
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }
}
