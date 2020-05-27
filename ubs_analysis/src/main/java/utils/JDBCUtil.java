package utils;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCUtil {

    //定义JDBC实例化所需要的固定参数
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://master:3036/db_telecom";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "puluyu123";

    /**
     * 实例化jdbc连接器对象
     */

    public static Connection getConnection() {

        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            return (Connection) DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return null;
    }


    public static void close(Connection connection, Statement statement, ResultSet resultSet) {


        try {
            if (resultSet != null && resultSet.isClosed()) {
                resultSet.close();
            }

            if (statement != null && statement.isClosed()) {
                statement.close();
            }
            if (connection != null && connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
