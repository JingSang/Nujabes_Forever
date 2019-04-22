package week_one;

import java.util.List;

public class 删除链表的倒数第N个节点 {
    /**给定一个链表，删除链表的倒数第 n 个节点，并且返回链表的头结点。

    示例：

     ListNode给定一个链表: 1->2->3->4->5, 和 n = 2.

    当删除了倒数第二个节点后，链表变为 1->2->3->5.
    说明：

    给定的 n 保证是有效的。**/
    public ListNode removeNthFromEnd(ListNode head, int n) {
        if(head==null||n<=0) return head;
        int c=0;
        ListNode cur=head;
        while(cur!=null){
            c++;
            cur=cur.next;
        }
        System.out.println(c);
        cur=head;c=c-n;
        while (c!=1){
            c--;
            cur=cur.next;
        }
        cur=cur.next.next;
        return head;
    }
    public ListNode removeNthFromEnd1(ListNode head, int n) {
        ListNode pre=new ListNode(0);
        ListNode next=head.next;
        pre.next=head;
        ListNode cur=head;
        int tmp=n;
        while(cur!=null){
            ListNode tmpNode=cur;
            ListNode tmpPre=pre;
            while(n>0&&tmpNode!=null){
                n--;
                tmpPre=tmpNode;
                tmpNode=tmpNode.next;
            }
            if(n==0&&tmpNode.next==null){
                pre.next=cur.next;
            }
            pre=cur;
            cur=cur.next;
        }
        return head;
    }
}
