package com.test.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ToHive {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public boolean run() {

        try {
            Class.forName(driverName);
            Connection con = null;
            //端口号默认为10000，根据实际情况修改；
            con = DriverManager.getConnection(
                    "jdbc:hive2://cdh4:2181,cdh5:2181,cdh6:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2_zk",
                    "songlei", "s84827409?");
            Statement stmt = con.createStatement();
            ResultSet res = null;
            String sql = "show databases";
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            System.out.println("ok");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            res.close();
            stmt.close();
            con.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error");

            return false;
        }
    }
    public static void main(String[] args) throws SQLException {
        ToHive hiveJdbcClient = new ToHive();
        hiveJdbcClient.run();
    }

}