package com.test.hive;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.sql.*;

public class Hive_Connector {
    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String CONNECTION_URL ="jdbc:hive2://cdh1:10000/rds;principal=hive/cdh1@EXAMPLE.COM";

    static {
        try {
            Class.forName(JDBC_DRIVER);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        //登录Kerberos账号
        System.setProperty("java.security.krb5.conf", "D:\\javaProject\\kafkaDemo\\target\\classes\\krb5.conf");
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.authentication" , "Kerberos" );
        UserGroupInformation. setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab("songlei/admin@EXAMPLE.COM", "D:\\javaProject\\kafkaDemo\\target\\classes\\songlei.keytab");

        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL);
            ps = connection.prepareStatement("show tables");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt(1));
            }

            rs.close();
            ps.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
