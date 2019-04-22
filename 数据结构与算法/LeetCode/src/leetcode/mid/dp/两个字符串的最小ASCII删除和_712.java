package leetcode.mid.dp;

public class 两个字符串的最小ASCII删除和_712 {

    public static int minimumDeleteSum(String s1, String s2) {
        int len1=s1.length();
        int len2=s2.length();
        char[] chas1=s1.toCharArray();
        char[] chas2=s2.toCharArray();
        //dp[i][j]0..i长度和0...j长度的字符串
        int[][] dp=new int[len1+1][len2+1];
        for(int i=1;i<=len2;i++){
            dp[0][i]+=(int)chas2[i-1]+dp[0][i-1];
            System.out.print(dp[0][i]+",");
        }
        System.out.println();
        for(int i=1;i<=len1;i++){
            dp[i][0]+=(int)chas1[i-1]+dp[i-1][0];
            System.out.print(dp[i][0]+",");
        }
        System.out.println();
        for(int i=1;i<=len1;i++){
            for (int j=1;j<=len2;j++){
                dp[i][j]=Math.min(dp[i-1][j]+(int)chas1[i-1],dp[i][j-1]+(int)chas2[j-1]);
                if(chas1[i-1]==chas2[j-1]){
                    dp[i][j]=Math.min(dp[i][j],dp[i-1][j-1]);
                }
                System.out.print(dp[i][j]+",");
            }
            System.out.println();
        }
        return dp[len1][len2];
    }

    public static void main(String[] args) {
        System.out.println(minimumDeleteSum("sea","eat"));
        System.out.println((int)'s'+","+(int)'e'+","+(int)'a'+","+(int)'t');
    }
}
