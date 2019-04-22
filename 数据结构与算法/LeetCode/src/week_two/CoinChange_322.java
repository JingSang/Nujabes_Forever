package week_two;

public class CoinChange_322 {
    public static int coinChange(int[] coins, int amount) {
        //dp[i][j]表示用i种零钱可以换j这么多钱的个数。
        int row=coins.length;
        int col=amount;
        int[][] dp=new int[row][col+1];
        dp[0][0]=0;
        for(int i=1;i<=col;i++){
            dp[0][i]=Integer.MAX_VALUE;
            if(i-coins[0]>=0&&dp[0][i-coins[0]]!=Integer.MAX_VALUE){
                dp[0][i]=dp[0][i-coins[0]]+1;
            }
            System.out.print(dp[0][i]+" ");
        }
        System.out.println();
        for(int i=1;i<row;i++){
            for (int j=1;j<=col;j++){
                dp[i][j]=dp[i-1][j];
                if(j-coins[i]>=0&&dp[i][j-coins[i]]!=Integer.MAX_VALUE){
                    dp[i][j]=Math.min(dp[i][j],dp[i][j-coins[i]]+1);
                }
                System.out.print(dp[i][j]+" ");
            }
            System.out.println();
        }
        return dp[row-1][col]==Integer.MAX_VALUE?-1:dp[row-1][col];
    }

    public static void main(String[] args) {
        int[] coins={2,5,10,1};
        System.out.println(coinChange(coins,27));
    }
}
