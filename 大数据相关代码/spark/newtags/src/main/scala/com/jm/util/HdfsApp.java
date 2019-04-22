package com.jm.util;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

public class HdfsApp {

    private static String output="F:/Tags/rcmd_article_content";
    private static String input="hdfs://10.70.2.56:8020/user/hive/warehouse/hive.db/rcmd_article_content/part-m-00001";

    private static FileSystem getFileSystem(String direPath)throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(direPath),configuration);
        return fileSystem;
    }

    private static  void readHdfsFile(String filePath){
        FSDataInputStream fsDataInputStream=null;
        try {
            Path path = new Path(filePath);
            fsDataInputStream = getFileSystem(filePath).open(path);
            OutputStream out = new FileOutputStream(output,true);
            IOUtils.copyBytes(fsDataInputStream,out,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(fsDataInputStream != null){
                IOUtils.closeStream(fsDataInputStream);
            }
        }
    }
    public static void getDirectoryFromHdfs(String direPath) throws Exception {
        FileSystem fs = getFileSystem(direPath);
        FileStatus[] filelist = fs.listStatus(new Path(direPath));
        for (int i = 0; i < filelist.length; i++) {
            System.out.println("_________________第" + i + "个文件" + "____________________");
            FileStatus fileStatus = filelist[i];
            System.out.println("Name:" + fileStatus.getPath().getName());
            System.out.println("Path:" + fileStatus.getPath());
            readHdfsFile(fileStatus.getPath().toString());
            System.out.println("size:" + fileStatus.getLen());
            System.out.println("_________________第" + i + "个文件" + "____________________");
        }
        fs.close();
    }
    public static void main(String[] args) throws Exception{
        HdfsApp hdfsApp = new HdfsApp();
        //hdfsApp.readHdfsFile("/user/data/JMRecommend/data-20181205-1/news/vectorize");
        ///user/hive/external/jiemianhomepage/dt=2018-12-12
        getDirectoryFromHdfs(input);
          //System.out.println(NLPTokenizer.segment("之岛"));
//        String inPath="D://workSpace/src/main/resources/hdfs-site.xml";
//        String outPath="hdfs://ns/user/kfk/data/local.xml";
        //hdfsApp.writeHdfsFile(inPath,outPath);
    }
}
