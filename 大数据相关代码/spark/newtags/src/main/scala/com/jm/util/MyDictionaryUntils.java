package com.jm.util;

import com.hankcs.hanlp.collection.trie.bintrie.BinTrie;
import com.hankcs.hanlp.dictionary.CoreDictionary;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.util.*;

public class MyDictionaryUntils {
    private  static String path="F:/Tags/我的词典.txt";
    private static int size=1;
    private static HashMap<String,String> map;
    private static File file=new File(path);
    private static int counter=0;

    public static boolean load() throws Exception{
        List<String> dic= FileUtils.readLines(file,"utf8");
        boolean flag=true;//
        for(int i=0;i<dic.size();i++) {
            String[] s=dic.get(i).split(" ");
            if (s[0].length()==0) continue;//排除空行
             flag=s.length==1?CustomDictionary.add(s[0]):CustomDictionary.add(s[0],linkStr(s));
             if(!flag){
                 //System.out.println("第"+(i+1)+"行加载出错:文件中有重复的词");
             }
        }
        return true;
    }
    public static boolean load(String path) throws Exception{
        File file=new File(path);
        List<String> dic= FileUtils.readLines(file,"utf8");
        boolean flag=true;//
        for(int i=0;i<dic.size();i++) {
            String[] s=dic.get(i).split(" ");
            if (s[0].length()==0) continue;//排除空行
            flag=s.length==1?CustomDictionary.add(s[0]):CustomDictionary.add(s[0],linkStr(s));
            if(!flag){
                System.out.println("第"+(i+1)+"行加载出错:文件中有重复的词");

            }
        }
        return true;
    }
    public static String linkStr(String[] strs){
        String value="";
        for (int i=1;i<strs.length;i++){
            value+=strs[i]+" ";
        }
        return value.substring(0,value.length()-1);
    }
    public static boolean add(String name){
       if(name==null||CustomDictionary.contains(name)) return false;
       return insert(name,null);
    }

    public static boolean add(String name,String natureWithFrequency){
        if(name==null||CustomDictionary.contains(name)) return false;
        return insert(name,natureWithFrequency);
    }

    public static boolean insert(String name){
        return insert(name,null);
    }

    public static boolean insert(String name,String natureWithFrequency){
        if(name==null) return false;
        if(CustomDictionary.insert(name,natureWithFrequency)) {
            if(map==null) map=new HashMap<>();
            map.put(name,natureWithFrequency==null?"":natureWithFrequency);
            if (map.size() >= size) save();
            return true;
        }else{
            return false;
        }
    }

    public static  void  save() {
        String content="";
        for (Map.Entry<String,String> entry:map.entrySet()){
            content+=entry.getValue().equals("")?entry.getKey()+"\r\n"
                                                :entry.getKey()+" "+entry.getValue()+"\r\n";
        }
        map.clear();
        //System.out.println("save111:"+content);
        appendMethodB(path,content,true);
    }
    public static void appendMethodB(String fileName, String content,boolean append) {
        try {
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            FileWriter writer = new FileWriter(fileName, append);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static boolean remove(String name) throws Exception{
        if(!CustomDictionary.contains(name)) return false;
        CustomDictionary.remove(name);
        if(map!=null&&map.containsKey(name))map.remove(name);
        if(++counter>=size){
            remove();
            counter=0;
        }
        return  true;
    }
    public static void remove(){
        BinTrie<CoreDictionary.Attribute> a=CustomDictionary.trie;
        Set<Map.Entry<String,CoreDictionary.Attribute>> set=a.entrySet();
        String content="";
        for (Map.Entry<String,CoreDictionary.Attribute> entry:set){
             content+=entry.getKey()+" "+entry.getValue()+"\r\n";
        }
        appendMethodB(path,content,false);
    }

}
