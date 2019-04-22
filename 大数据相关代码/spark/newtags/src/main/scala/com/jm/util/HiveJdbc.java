package com.jm.util;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class HiveJdbc {

    public static void main(String[] args) throws IOException {

    }

    /**
     * 将数据插入hdfs中，用于load到hive表中，默认分隔符是"\001"
     * @param dst
     * @param contents
     * @throws IOException
     */
    public static void createFile(String dst , List<List> argList) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        StringBuffer sb = new StringBuffer();
        for(List<String> arg:argList){
            for(String value:arg){
                sb.append(value).append("\001");
            }
            sb.deleteCharAt(sb.length() - 4);//去掉最后一个分隔符
            sb.append("\n");
        }
        sb.deleteCharAt(sb.length() - 2);//去掉最后一个换行符
        byte[] contents =  sb.toString().getBytes();
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }
    /**
     * 将HDFS文件load到hive表中
     * @param dst
     */
    public static void loadData2Hive(String dst) {
        String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
        String CONNECTION_URL = "jdbc:hive2://server-13:10000/default;auth=noSasl";
        String username = "admin";
        String password = "admin";
        Connection con = null;

        try {
            Class.forName(JDBC_DRIVER);
            con = (Connection) DriverManager.getConnection(CONNECTION_URL,username,password);
            Statement stmt = con.createStatement();

            String sql = " load data inpath '"+dst+"' into table population.population_information ";

            stmt.execute(sql);
            System.out.println("loadData到Hive表成功！");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }finally {
            // 关闭rs、ps和con
            if(con != null){
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}