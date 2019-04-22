package leetcode.mid.dp;

public class 等差数列划分_413 {
    public int numberOfArithmeticSlices(int[] A) {
        if(A.length<=2) return 0;
        int len=A.length;
        //dp[i][j] 数组  i....j  位置的个数
        //返回的结果就是0....j
        int[][] dp=new int[len][len];
        //初始化
        for(int j=len-1;j>=0;j--){
            for (int i=j;i<len;i++){

            }
        }
        return 0;
    }
}
