package simpledb.transaction;

import simpledb.common.Permissions;

import java.util.Objects;

public class LockState {
    private TransactionId tid;
    private Permissions perm;
    public LockState(TransactionId tid,Permissions perm){
        this.tid=tid;
        this.perm=perm;
    }
    public TransactionId getTid(){return tid;};
    public Permissions getPerm(){return perm;};

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockState lockState = (LockState) o;
        return tid.equals(lockState.tid) &&
                perm == lockState.perm;
    }

    @Override
    public int hashCode() {
        int res=tid.hashCode();
        res=31*res+perm.hashCode();
        return res;
    }
}
