package cn.thinkingdata.kafka.consumer.persist;

import cn.thinkingdata.kafka.constant.KafkaMysqlOffsetParameter;
import cn.thinkingdata.kafka.util.CommonUtils;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;


public class DBPoolConnection {

    private static final Logger logger = LoggerFactory.getLogger(MysqlOffsetPersist.class);
    private static DBPoolConnection dbPoolConnection = null;
    private static DruidDataSource druidDataSource = null;

    static {
        try {
            Properties properties = new Properties();
//            properties.load(DBPoolConnection.class.getResourceAsStream("/db_server.properties"));
            properties.put("driverClassName", "com.mysql.jdbc.Driver");
            properties.put("url", KafkaMysqlOffsetParameter.jdbcUrl);
            properties.put("username", KafkaMysqlOffsetParameter.username);
            properties.put("password", KafkaMysqlOffsetParameter.password);
            properties.put("filters", "stat");
            properties.put("initialSize", "1");
            properties.put("minIdle", "1");
            properties.put("maxActive", "30");
            properties.put("maxWait", "60000");
            properties.put("timeBetweenEvictionRunsMillis", "60000");
            properties.put("minEvictableIdleTimeMillis", "300000");
            properties.put("validationQuery", "SELECT 1");
            properties.put("testWhileIdle", "true");
            properties.put("testOnBorrow", "false");
            properties.put("testOnReturn", "false");
            properties.put("poolPreparedStatements", "true");
            properties.put("maxPoolPreparedStatementPerConnectionSize", "20");
            druidDataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            logger.error("get druidDataSource error, the error is " + CommonUtils.getStackTraceAsString(e));
            System.exit(-1);
        }
    }

    /**
     * 数据库连接池单例
     *
     * @return
     */
    public static synchronized DBPoolConnection getInstance() {
        if (null == dbPoolConnection) {
            dbPoolConnection = new DBPoolConnection();
        }
        return dbPoolConnection;
    }

    private DBPoolConnection() {
    }

    /**
     * 返回druid数据库连接
     *
     * @return
     * @throws SQLException
     */
    public DruidPooledConnection getConnection() throws SQLException {
        return druidDataSource.getConnection();
    }

    public void close() {
        druidDataSource.close();
    }

}
