package com.test.impala;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.*;

public class Toimpala {
    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
//    private static String CONNECTION_URL = "jdbc:impala://cdh1:21050/ods;AuthMech=3;KrbRealm=EXAMPLE.COM;KrbHostFQDN=cdh1;KrbServiceName=impala";
    private static String CONNECTION_URL = "jdbc:impala://172.17.0.198:21050/;AuthMech=3;";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL,"songlei", "s84827409?");
            ps = connection.prepareStatement("show databases");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if(rs !=null)rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if(ps !=null)ps.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if(connection !=null)connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
