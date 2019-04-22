package leetcode.mid;

public class 不同的二叉搜索树_96 {

    public static int numTrees(int n) {
        if(n<=2) return n;
        int[] res=new int[n+2];
        res[0]=1;
        res[1]=1;
        for(int i=2;i<=n;i++){
            int tmp=0;
            for (int j=1;j<=i;j++){
                tmp += (res[j - 1]) * (res[i - j]);
                System.out.println(res[j - 1]+","+res[i - j]);
            }
            res[i]=tmp;
        }
        return res[n];
    }

    public static void main(String[] args) {
        System.out.println(numTrees(6));
    }
}
