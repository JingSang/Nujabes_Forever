package leetcode.mid;

import sun.reflect.generics.tree.Tree;

public class 最大二叉树_654 {

    public static TreeNode constructMaximumBinaryTree(int[] nums) {
        if(nums.length==1) return new TreeNode(nums[0]);
        int left=0;
        int right=0;
        int len=nums.length;
        int max=Integer.MIN_VALUE;
        for(int i=0;i<len;i++){
            if(nums[i]>max){
                left=i;
                right=i+1;
                max=nums[i];
            }
        }
        TreeNode root=new TreeNode(max);
        System.out.println("主节点:"+root==null?"null":root.val);
        TreeNode leftNode=construcctNode(nums,0,left);
        System.out.println("左节点:"+leftNode==null?"null":leftNode.val);
        TreeNode rightNode=construcctNode(nums,right,len);
        System.out.println("右节点:"+rightNode==null?"null":rightNode.val);
        root.left=leftNode;
        root.right=rightNode;
        return root;
    }
    public static TreeNode construcctNode(int[] nums,int start,int end){
        if(start==end||start>end) return new TreeNode(-1);
        int left=start;
        int right=start;
        int len=end;
        int max=Integer.MIN_VALUE;
        for(int i=start;i<end;i++){
            if(nums[i]>max){
                left=i;
                right=i+1;
                max=nums[i];
            }
        }
        TreeNode root=new TreeNode(max);
        System.out.println("主节点:"+root==null?"null":root.val);
        TreeNode leftNode=construcctNode(nums,start,left);
        System.out.println("左节点:"+leftNode==null?"null":leftNode.val);
        TreeNode rightNode=construcctNode(nums,right,end);
        System.out.println("右节点:"+rightNode==null?"null":rightNode.val);
        root.left=leftNode;
        root.right=rightNode;
        return root;
    }

    public static void main(String[] args) {
        int[] res={3,2,1,6,0,5};
        constructMaximumBinaryTree(res);
    }

}
