package simpledb.transaction;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public static class PageLock {//定义PageLock类，主要变量有pid和perm
        public final PageId pid;
        public Permissions perm;
        public int holdNum;

        public PageLock(PageId pid, Permissions perm) {
            this.pid = pid;
            this.perm = perm;
            this.holdNum = 1;
        }

        public boolean equals(Object o) {
            //ATTENTION! 此处的equal指的是pid相同
            if (!(o instanceof PageLock))
                return false;
            if (this == o) return true;
            return this.pid.equals((PageLock) ((PageLock) o).pid);
        }

        public Permissions getPerm() {
            return perm;
        }

        public int hashCode() {
            return pid.hashCode();
        }
    }

    static class Digraph {
        final ConcurrentHashMap<TransactionId, HashSet<TransactionId>> waitList;

        public Digraph() {
            waitList = new ConcurrentHashMap<>();
        }

        void print() {
            for (Map.Entry e : waitList.entrySet()) {
                System.out.print("" + ((TransactionId) e.getKey()).getId() + ": ");
                HashSet<TransactionId> s = (HashSet<TransactionId>) e.getValue();
                for (TransactionId i : s) {
                    System.out.print("" + i.getId() + ", ");
                }
                System.out.println();
            }
        }

        public void addVertex(TransactionId tid) {
            if (waitList.containsKey(tid)) {
                return;
            }
            waitList.put(tid, new HashSet<>());
        }

        public void addEdge(TransactionId from, TransactionId to) {
            addVertex(from);
            addVertex(to);
            waitList.get(from).add(to);
        }

        public void removeVertex(TransactionId tid) {
            for (Map.Entry e : waitList.entrySet()) {
                HashSet<TransactionId> s = (HashSet<TransactionId>) e.getValue();
                s.remove(tid);
            }
            waitList.remove(tid);
        }

        public void removeEdge(TransactionId from, TransactionId to) {
            if (waitList.containsKey(from) && waitList.containsKey(to))
                waitList.get(from).remove(to);
        }

        private boolean isCyclicHelper(TransactionId id, ConcurrentHashMap<TransactionId, Boolean> visited,
                                       ConcurrentHashMap<TransactionId, Boolean> traceStack) {//基于深度优先
            if (traceStack.getOrDefault(id, false))//路径中本来就有这个点，第二次了，有环
                return true;
            if (visited.getOrDefault(id, false))//不在栈但被访问过，不是圈
                return false;
            visited.put(id, true);//记录访问状态
            traceStack.put(id, true);//放入栈
            Set<TransactionId> s = waitList.get(id);//与id为相邻节点的集合
            for (TransactionId t : s)
                if (isCyclicHelper(t, visited, traceStack)) {
                    return true;
                }
            traceStack.put(id, false);//走出for语句块，确保id不是环，可以出栈
            return false;
        }

        public boolean isCyclic() {
            int v = waitList.size();
            ConcurrentHashMap<TransactionId, Boolean> visited = new ConcurrentHashMap<>();
            ConcurrentHashMap<TransactionId, Boolean> tracestack = new ConcurrentHashMap<>();
            for (TransactionId id : waitList.keySet())//遍历顶点
                if (isCyclicHelper(id, visited, tracestack))//任何一个函数返回true，都是isCyclic
                    return true;
            return false;
        }
    }


    final ConcurrentHashMap<TransactionId, Set<PageLock>> txn2LocksMap;
    final ConcurrentHashMap<PageId, Set<TransactionId>> pageId2TxnsMap;
    final Digraph graph;
    volatile boolean hasWriter = false;
    volatile PageId writerPage;

    public LockManager() {
        txn2LocksMap = new ConcurrentHashMap<>();
        pageId2TxnsMap = new ConcurrentHashMap<>();
        graph = new Digraph();
    }

    public synchronized void grantLock(TransactionId tid, PageId pid, Permissions permType) throws TransactionAbortedException {
        Set<TransactionId> txns = pageId2TxnsMap.get(pid);//当前page的事务集合
        Set<PageLock> locks = txn2LocksMap.get(tid);//tid持有的锁                                                                                              前事务持有的锁集合
        //页面还没有事务
        if (txns == null) {
            PageLock lock = new PageLock(pid, permType);
            txns = new HashSet<>();
            txns.add(tid);
            pageId2TxnsMap.put(pid, txns);
            if (locks == null) {//没有任何锁，生成一个新的锁集合
                locks = new HashSet<>();
                locks.add(lock);
                txn2LocksMap.put(tid, locks);//事务得到的第一个锁
            }
            else {
                locks.add(lock);
                txn2LocksMap.put(tid, locks);
            }
        }
        //除tid还有事务在当前页面
        else {
            if(permType.equals(Permissions.READ_ONLY)){
                if(locks!=null && locks.contains(new PageLock(pid,Permissions.READ_ONLY))){
                    //当前事务对此页本来就有读锁，无需再改变
                    return;
                }
                if(locks != null && hasWriter && writerPage.equals(pid)){
                    //页面正在被写，不可申请读锁
                    throw new TransactionAbortedException();
                }
                PageLock plk = null;
                for(TransactionId txnId : txns){//遍历当前页面的事务
                    Set<PageLock> currentLocks = txn2LocksMap.get(txnId);//每个事务得到锁集合
                    if(currentLocks != null){
                        for(PageLock p : currentLocks){
                            if(p.pid == pid){//找到一个别的事务对此页的锁
                                plk = p;
                                break;
                            }
                        }
                    }
                    if(plk != null) {//循环是为了找到同一页的其他锁，找到就退出循环
                        break;
                    }
                }
                if(plk != null && plk.perm == Permissions.READ_WRITE){//别的事务在此页有写锁
                    for(TransactionId txnId : txns){
                        if(!txnId.equals(tid)){
                            graph.addEdge(tid,txnId);//用等待图表达等待关系，addEdge实际上就是做到了等待
                        }
                    }
                    if(graph.isCyclic()){//有圈，会死锁，去除刚加入的点、边，抛出异常
                        for(TransactionId id:txns){
                            graph.removeEdge(tid,id);
                        }
                        graph.removeVertex(tid);
                        throw new TransactionAbortedException();
                    }
                    while(plk.holdNum!=0){
                        try{
                            this.wait();
                        }catch(InterruptedException e){

                        }
                    }
                    plk.holdNum++;
                    plk.perm = Permissions.READ_ONLY;
                    graph.removeVertex(tid);
                    if(locks==null){
                        locks=new HashSet<>();
                        locks.add(plk);
                        txn2LocksMap.put(tid,locks);
                    }
                    else {
                        locks.add(plk);
                    }
                    txns.add(tid);
                }

            }

            else {//申请的是写锁
                boolean holds = false;//有没有拿住表的锁状态
                PageLock plk = null;
                for (TransactionId txnId : txns) {//老样子得到别的事务在此页的一个锁
                    Set<PageLock> currentLocks = txn2LocksMap.get(txnId);
                    if (currentLocks != null) {
                        for (PageLock p : currentLocks) {
                            if (p.equals(new PageLock(pid, permType))) {
                                plk = p;
                                break;
                            }
                        }
                    }
                    if (plk != null) {
                        break;
                    }
                }
                if (txns.contains(tid)) {
                    if (txns.size() == 1) {
                        if (plk != null && plk.perm.equals(Permissions.READ_ONLY)) {//就一个读锁，升级就行
                            plk.perm = Permissions.READ_WRITE;
                            plk.holdNum = 1;
                        }
                        //else do nothing
                        return;
                    } else {
                        holds = true;
                    }
                }
                for (TransactionId txnId : txns) {
                    if (!txnId.equals(tid)) {
                        graph.addEdge(tid, txnId);
                    }
                }
                if (graph.isCyclic()) {
                    for (TransactionId id : txns) {
                        graph.removeEdge(tid, id);
                    }
                    graph.removeVertex(tid);
                    throw new TransactionAbortedException();
                }
                //准备开始写
                hasWriter = true;
                writerPage = pid;
                while (plk.holdNum != 0 && (plk.holdNum != 1 || !holds)) {//等待
                    try {
                        this.wait();
                    } catch (InterruptedException e) {

                    }
                }
                hasWriter = false;//写完了
                writerPage = null;
                graph.removeVertex(tid);
                updateMap(plk, permType, tid, locks);
                txns.add(tid);
            }
        }
    }

    public synchronized  void updateMap(PageLock plk,Permissions perm, TransactionId tid,Set<PageLock> locks){
        plk.holdNum++;
        plk.perm = perm;
        if(locks == null){
            locks = new HashSet<>();
            locks.add(plk);
            txn2LocksMap.put(tid,locks);
        }
        else{
            locks.add(plk);
        }
    }
    public synchronized boolean holdLock(TransactionId tid,PageId pid){
        return pageId2TxnsMap.containsKey(pid) && pageId2TxnsMap.get(pid)!=null &&pageId2TxnsMap.get(pid).contains(tid);
    }
    public synchronized void releaseLock(TransactionId tid,PageId pid){
        if(holdLock(tid,pid)){
            Set<PageLock> lockSet = txn2LocksMap.get(tid);
            Set<TransactionId> txnSet = pageId2TxnsMap.get(pid);
            PageLock lock = null;
            PageLock targetLock = new PageLock(pid,Permissions.READ_WRITE);
            for(PageLock l : lockSet){
                if(l.equals(targetLock)){
                    lock = l;
                }
            }
            lock.holdNum--;
            txnSet.remove(tid);
            if(txnSet.size()==0){
                pageId2TxnsMap.remove(pid);
            }
            lockSet.remove(lock);
            if(lockSet.size()==0){
                txn2LocksMap.remove(tid);
            }
            this.notifyAll();
        }
    }
    public synchronized Set<PageLock> getPages(TransactionId tid){
        return new HashSet<>(txn2LocksMap.getOrDefault(tid,Collections.emptySet()));
    }
}



