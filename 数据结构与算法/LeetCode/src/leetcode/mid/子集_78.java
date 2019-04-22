package leetcode.mid;

import java.util.ArrayList;
import java.util.List;

public class 子集_78 {
    static List<List<Integer>> res=new ArrayList<>();
    public static List<List<Integer>> subsets(int[] nums) {
        if(nums.length==0||nums==null) return res;
        List<Integer> list=new ArrayList<>();
        boolean[] flag=new boolean[nums.length];
        recursion(nums,flag,0,list);
        return res;
    }
    public static void recursion(int[] nums,boolean[] flag,int index,List<Integer> list){
        if(!res.contains(list)){
            res.add(new ArrayList<>(list));
        }
        if(index==nums.length){
            return;
        }
        for(int i=index;i<nums.length;i++){
            if(!flag[i]){
                list.add(nums[i]);
                flag[i]=true;
                recursion(nums,flag,++index,list);
                list.remove(list.size()-1);
                flag[i]=false;
            }
        }
    }

    public static void main(String[] args) {
        int[] res={1,2,3};
        subsets(res);
    }
}
