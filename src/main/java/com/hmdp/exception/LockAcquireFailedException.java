package com.hmdp.exception;

/**
 * 分布式锁获取失败异常（运行时异常，无需强制捕获）
 */
public class LockAcquireFailedException extends RuntimeException {
    public LockAcquireFailedException(String message) {
        super(message);
    }

    public LockAcquireFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}

