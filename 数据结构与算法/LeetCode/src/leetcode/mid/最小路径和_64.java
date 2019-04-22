package leetcode.mid;

public class 最小路径和_64 {
    public int minPathSum(int[][] grid) {
        if(grid==null||grid[0]==null) return -1;
        int row=grid.length;
        int col=grid[0].length;
        int[][] res=new int[row][col];
        res[0][0]=grid[0][0];
        for(int i=1;i<row;i++){
            res[i][0]=res[i-1][0]+grid[i][0];
        }
        for (int j=1;j<col;j++){
            res[0][j]=res[0][j-1]+grid[0][j];
        }
        for (int i=1;i<row;i++){
            for (int j=1;j<col;j++){
                res[i][j]=Math.min(res[i-1][j],res[i][j-1])+grid[i][j];
            }
        }
        return res[row-1][col-1];
    }
}
