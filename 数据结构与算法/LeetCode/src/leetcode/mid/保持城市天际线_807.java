package leetcode.mid;

public class 保持城市天际线_807 {

    public int maxIncreaseKeepingSkyline(int[][] grid) {
        int row_len=grid.length;
        int col_len=grid[0].length;
        int[] col=new int[col_len];
        int[] row=new int[row_len];
        System.out.println(row_len);
        System.out.println(col_len);
        for(int i=0;i<row_len;i++){
            for(int j=0;j<col_len;j++){
                col[i]=Math.max(grid[i][j],col[i]);
                row[j]=Math.max(grid[i][j],row[j]);
            }
        }
        for(int i=0;i<row_len;i++){
            System.out.println(row[i]+",");
        }
        for(int i=0;i<col_len;i++){
            System.out.print(col[i]+",");
        }
        int res=0;
        for(int i=0;i<row_len;i++){
            for (int j=0;j<col_len;j++){
                res+=Math.min(row[i],col[j])-grid[i][j];
            }
        }
        return res;
    }
}
