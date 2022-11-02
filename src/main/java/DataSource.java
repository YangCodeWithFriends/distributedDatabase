import com.datastax.oss.driver.api.core.CqlSession;
import com.yugabyte.ysql.YBClusterAwareDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 3:21 PM
 */
public class DataSource {
    private HikariConfig config;
    private HikariDataSource ds;
//    private YBClusterAwareDataSource ds;

    private CqlSession session;
    public static final String YSQL = "YSQL";
    public static final String YCQL = "YCQL";


    public DataSource(String MODE, int serverIndex, Logger logger) {
        Properties settings = new Properties();
        try {
            settings.load(SampleApp.class.getResourceAsStream("app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
            logger.log(Level.SEVERE,"Load app.properties exception= ", e);
        }

        int hostID = serverIndex % 5;
        String hostKey = "host" + hostID;
        logger.log(Level.SEVERE, String.format("serverIndex=%d,hostID=%d,hostKey=%s,host=%s\n",serverIndex,hostID,hostKey,settings.getProperty(hostKey)));

        if (MODE.equals(YSQL)) {
            Properties poolProperties = new Properties();
//            poolProperties.setProperty("dataSourceClassName", "com.yugabyte.ysql.YBClusterAwareDataSource");
            poolProperties.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
            poolProperties.setProperty("maximumPoolSize", "20");
            poolProperties.setProperty("dataSource.serverName", settings.getProperty(hostKey));
            poolProperties.setProperty("dataSource.portNumber", settings.getProperty("port_sql"));
            poolProperties.setProperty("dataSource.databaseName", "dbysql");
            poolProperties.setProperty("dataSource.user", settings.getProperty("dbUser"));
            poolProperties.setProperty("dataSource.password", settings.getProperty("dbPassword"));
            poolProperties.setProperty("poolName", "HikariCP");

            config = new HikariConfig(poolProperties);

            /*
//            config = new HikariConfig();
//            config.setDataSourceClassName("com.nuodb.jdbc.Driver");
            config.setUsername(settings.getProperty("dbUser"));
            config.setPassword(settings.getProperty("dbPassword"));
            config.addDataSourceProperty( "cachePrepStmts" , "true" );
            config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
            config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
            */

            String jdbcUrl = "jdbc:yugabytedb://" + settings.getProperty(hostKey) + ":"
                    + settings.getProperty("port_sql") + "/dbysql";
            config.setJdbcUrl(jdbcUrl);
            config.validate();

            ds = new HikariDataSource(config);
//            System.out.println("Connect to DB successfully");

            /*
            ds = new YBClusterAwareDataSource();
            ds.setUrl("jdbc:yugabytedb://" + settings.getProperty(hostKey) + ":"
                    + settings.getProperty("port_sql") + "/yugabyte");
//            ds.setURL(jdbcUrl);
            ds.setUser(settings.getProperty("dbUser"));
            System.out.println("URL = " + ds.getURL());
            ds.setPassword(settings.getProperty("dbPassword"));
            ds.setDatabaseName("dbysql");
            int portNumber = Integer.parseInt(settings.getProperty("port_sql"));
            System.out.println("port= " + portNumber);
//            ds.setPortNumbers(new int[portNumber]);
             */
        } else if (MODE.equals(YCQL)) {
            session = CqlSession
                    .builder()
                    .addContactPoint(new InetSocketAddress(settings.getProperty(hostKey), Integer.parseInt(settings.getProperty("port_cql"))))
                    .build();
//            System.out.println("Connect to DB successfully");
        }
        else throw new RuntimeException("mode in app.properties has to be YCQL/YSQL!");
    }


    // YCQL
    public CqlSession getCQLSession() {
        return session;
    }

    // YSQL
    public Connection getSQLConnection() throws SQLException {
        return ds.getConnection();
    }
}
