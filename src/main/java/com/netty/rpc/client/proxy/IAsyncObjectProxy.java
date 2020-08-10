package com.netty.rpc.client.proxy;

import com.netty.rpc.client.RPCFuture;

public interface IAsyncObjectProxy {
    public RPCFuture call(String funcName, Object... args);
}