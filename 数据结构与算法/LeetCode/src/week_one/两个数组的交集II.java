package week_one;

import java.util.ArrayList;
import java.util.HashMap;

public class 两个数组的交集II {
    public static  int[] intersect(int[] nums1, int[] nums2) {
        HashMap<Integer,Integer> record=new HashMap<>();
        for(int i=0;i<nums1.length;i++){
            if(!record.containsKey(nums1[i])){
                record.put(nums1[i],1);
            }else {
                int val=record.get(nums1[i]);
                record.put(nums1[i],++val);
            }
        }
        ArrayList<Integer> list=new ArrayList<>();
        for(int i=0;i<nums2.length;i++){
            if(record.containsKey(nums2[i])&&record.get(nums2[i])>0){
                list.add(nums2[i]);
                int val=record.get(nums2[i]);
                record.put(nums2[i],--val);
            }
        }
        int[] res=new int[list.size()];
        for(int i=0;i<list.size();i++){
            res[i]=list.get(i);
        }
        return res;
    }

    public static void main(String[] args) {
        int[] a={1,2,2,1};
        int[] b={2,2};
        intersect(a,b);
    }
}
