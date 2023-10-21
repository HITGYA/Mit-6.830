package simpledb.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//        LRU是一种常见的缓存淘汰算法。基本思想是，当缓存空间已满时，优先淘汰最近最少被访问的数据。
//        LRU 算法的实现方式通常是使用一个双向链表和一个哈希表结合的数据结构：
//          双向链表：用于记录数据项的访问顺序。最近访问的数据项在链表的头部，最近最少未被访问的数据项在链表的尾部。
//                  用双向链表是为了保证插入删除的高效率
//          哈希表：用于快速查找数据项。哈希表的键是数据项的标识符，值是指向对应链表节点的指针。
//
//        当数据项被访问时，LRU 算法的操作如下：
//        如果数据项在缓存中存在，将其从链表中移到链表头部，表示最近被访问。
//        如果数据项不在缓存中，需要进行以下操作：
//        如果缓存已满，淘汰链表尾部的数据项，即最久未被访问的数据项。在链表头部插入新的数据项。
public class LRUCache<K,V> {
    class DLinkedNode{//双向链表的节点
        K key;
        V value;
        DLinkedNode pre;
        DLinkedNode next;
        DLinkedNode(){};
        DLinkedNode(K k,V v){this.key=k;this.value=v;}
    }
    private int capacity;
    private int size;
    private Map<K,DLinkedNode> cache = new ConcurrentHashMap<K,DLinkedNode>();
    private DLinkedNode head,tail;
    public LRUCache(int capacity){
        this.size = 0;
        this.capacity = capacity;
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        head.pre =tail;
        tail.next=head;
        tail.pre=head;
    }
    public int getSize(){return size;}
    public DLinkedNode getHead(){return head;}
    public DLinkedNode getTail(){return tail;}
    public Map<K,DLinkedNode> getCache(){return cache;}
    /**
     * 根据key获取value
     * @param key
     * @return
     */
    public synchronized V get(K key){//LRUCache级别get
        DLinkedNode node = cache.get(key);
        if(node==null) return null;
        moveToHead(node);
        return node.value;
    }
    public synchronized void  put(K key ,V val){
         DLinkedNode node = cache.get(key);
         if(node != null){
             //修改
             node.value=val;
             moveToHead(node);
         }
         else {
             //新增
             DLinkedNode newnode = new DLinkedNode(key,val);
             this.cache.put(key,newnode);
             addTohead(newnode);
             this.size++;
             if(size>capacity){//删除最后一个
                 DLinkedNode tmp = tail.pre;
                 removeNode(tmp);
                 this.cache.remove(key);
                 size--;
             }
         }
    }
    public void remove(DLinkedNode node){//从整个LRUCache删除，链表哈希表都删
        removeNode(node);
        cache.remove(node);
        size--;
    }
    private void moveToHead(DLinkedNode node){//链表操作
        removeNode(node);
        addTohead(node);
    }
    private void removeNode(DLinkedNode node){//链表操作
        node.pre.next = node.next;
        node.next.pre = node.pre;
    }
    private void addTohead(DLinkedNode node){//链表操作
        node.next=head.next;
        node.pre = head;
        head.next.pre = node;
        head.next = node;
    }
    public synchronized void discard(){
        DLinkedNode tail = removeTail();
        cache.remove(tail.key);
        size--;
    }
    private DLinkedNode removeTail(){
        DLinkedNode res = tail.pre;
        remove(res);
        return res;
    }
}
