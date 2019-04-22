package com.test.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.*;

public class ToHive_1 {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public boolean run() {
        Connection con = null;
        Statement stmt = null;
        ResultSet res =  null;
        try {
            Class.forName(driverName);
            Configuration conf =new Configuration();
            conf.set("hadoop.security.authentication","Kerberos");
            System.setProperty("java.security.krb5.conf", "D:\\javaProject\\kafkaDemo\\target\\classes\\krb5.conf");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("songlei/admin@SXKJ.COM", "D:\\javaProject\\kafkaDemo\\target\\classes\\songlei.keytab");

            //端口号默认为10000，根据实际情况修改；
            con = DriverManager.getConnection(
                    "jdbc:hive2://cdh1:10000/;principal=hive/cdh1@SXKJ.COM");
            stmt = con.createStatement();

            String sql = "show databases";
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            System.out.println("ok");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error");
        }finally {
            try {
                if(res !=null)res.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if(stmt !=null)stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if(con !=null)con.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }
    public static void main(String[] args) throws SQLException {
        ToHive_1 hiveJdbcClient = new ToHive_1();
        hiveJdbcClient.run();
    }
}
