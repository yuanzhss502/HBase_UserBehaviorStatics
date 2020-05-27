package utils;

import com.mysql.jdbc.Connection;

import java.sql.SQLException;

public class JDBCInstance {

    private static Connection connection = null;
    private JDBCInstance(){};
    public Connection getConnection() {

        try {
            if (connection == null | connection.isClosed()) {

                connection = JDBCUtil.getConnection();

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

}
