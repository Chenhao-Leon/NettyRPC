package com.netty.rpc.client;

public interface AsyncRPCCallback {

    void success(Object result);

    void fail(Exception e);

}
