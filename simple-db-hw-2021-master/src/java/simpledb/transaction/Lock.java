package simpledb.transaction;

import simpledb.common.Permissions;

public class Lock {
    private TransactionId transactionId;
    private Permissions permissions;
    public Lock(TransactionId tid , Permissions permissions){
        this.transactionId = tid ;
        this.permissions = permissions ;
    }
    public TransactionId getTransactionId(){
        return transactionId;
    }
    public Permissions getPermissions(){
        return permissions;
    }
    public void setPermissions(Permissions p){
        this.permissions = p;
    }
    public String toString(){
        return  "Lock{" +
                "permissions=" + permissions +
                ", transactionId=" + transactionId +
                '}';
    }
}
