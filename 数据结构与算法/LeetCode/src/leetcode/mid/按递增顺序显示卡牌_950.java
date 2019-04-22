package leetcode.mid;

import java.util.Arrays;

public class 按递增顺序显示卡牌_950 {


    public int[] deckRevealedIncreasing(int[] deck) {
        Arrays.sort(deck);
        if(deck.length==2||deck.length==1){
            return deck;
        }
        int len=deck.length;
        int[] res=new int[len];
        res[len-1]=deck[len-1];
        res[len-2]=deck[len-2];
        for(int i=len-3;i>-1;i--){
            insert(res,deck[i],i);
        }
        return res;
    }

    public void insert(int[] res,int val,int index){
        res[index]=val;
        resort(res,index+1);
    }
    public void resort(int[] res,int i){
        int len=res.length;
        int tmp=res[len-1];
        for(int j=len-1;j>i;j--){
            res[j]=res[j-1];
        }
        res[i]=tmp;
    }
}
