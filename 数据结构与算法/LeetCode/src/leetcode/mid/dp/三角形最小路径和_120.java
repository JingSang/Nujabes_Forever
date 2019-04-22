package leetcode.mid.dp;

import java.util.Arrays;
import java.util.List;

public class 三角形最小路径和_120 {

    public int minimumTotal(List<List<Integer>> triangle) {
        if(triangle.size()==1) return triangle.get(0).get(0);
        int len=triangle.size();
        //dp[i]=某一层i位置最小数。
        int[] dp=new int[len];
        int[] tmp=new int[len];
        int res=Integer.MAX_VALUE;
        dp[0]=triangle.get(0).get(0);
        tmp[0]=dp[0];
        //从第一层开始
        for (int i=1;i<len;i++){
            int cur_len=triangle.get(i).size();
            System.out.println(cur_len);
            for(int j=0;j<cur_len;j++) {
                int cur_val = triangle.get(i).get(j);
                tmp[j] = j == 0 ? cur_val + dp[j] :
                        j == cur_len - 1 ? cur_val + dp[j - 1] :
                                Math.min(dp[j - 1] + cur_val, dp[j] + cur_val);
                res=i==len-1?Math.min(tmp[j],res):res;
                System.out.print(tmp[j]+",");
            }
            for (int j=0;j<cur_len;j++){
                dp[i]=tmp[i];
            }
            System.out.println();
        }
        return res;
    }

}
