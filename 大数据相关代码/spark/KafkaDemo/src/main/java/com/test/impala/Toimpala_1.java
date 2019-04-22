package com.test.impala;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Toimpala_1 {
    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL = "jdbc:impala://cdh1:21050/;AuthMech=1;KrbRealm=BEADWALLET.COM;KrbHostFQDN=cdh1;KrbServiceName=impala";
    //private static String CONNECTION_URL = "jdbc:impala://172.17.0.198:21050/ods/;AuthMech=3;";


    public static void main(String[] args) {

        try {
            Configuration conf =new Configuration();
            conf.set("hadoop.security.authentication","Kerberos");
            System.setProperty("java.security.krb5.conf", "D:\\javaProject\\kafkaDemo\\target\\classes\\krb5.conf");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("songlei/admin@SXKJ.COM", "D:\\javaProject\\kafkaDemo\\target\\classes\\songlei.keytab");

            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

            loginUser.doAs(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    Connection connection = null;
                    ResultSet rs = null;
                    PreparedStatement ps = null;
                    try{
                        Class.forName(JDBC_DRIVER);
                        connection = DriverManager.getConnection(CONNECTION_URL);
                        ps = connection.prepareStatement("show databases");
                        rs = ps.executeQuery();
                        while (rs.next()) {
                            System.out.println(rs.getString(1));
                        }
                    }catch (Exception e){
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
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
