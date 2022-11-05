import com.datastax.oss.driver.api.core.CqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 5/11/22 2:59 PM
 */
public class DBState {
    public static void main(String[] args) throws SQLException {
        Connection conn = null;
        CqlSession cqlSession = null;
        String MODE = DataSource.YCQL;
        int serverIndex = 1;
        Logger logger = Logger.getLogger(Thread.currentThread().getName());
        ExecuteManager executeManager = new ExecuteManager();

        try {
            if (MODE.equals(DataSource.YSQL)) {
                logger.log(Level.WARNING, "Connecting to DB. Your mode is YSQL.");
                conn = new DataSource(MODE, serverIndex, logger).getSQLConnection();
                logger.log(Level.INFO, "Conn = " + conn.getClientInfo());
//                logger.log(Level.INFO, "Isolation level=" + conn.getTransactionIsolation());
            } else {
                logger.log(Level.WARNING, "Connecting to DB. Your mode is YCQL.");
                cqlSession = new DataSource(MODE, serverIndex, logger).getCQLSession();
                executeManager.reportCQL(cqlSession, logger);
                logger.log(Level.INFO, "CQLSession = " + cqlSession.getName());
            }
            logger.log(Level.SEVERE, "ok");
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "DB Connection exception= ", e);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "DB Connection general exception= ", e);
        } finally {
            if (MODE.equals(DataSource.YCQL) && cqlSession != null) cqlSession.close();
            else conn.close();
        }
        logger.log(Level.SEVERE, "END");
    }

}
