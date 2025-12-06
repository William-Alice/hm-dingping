package com.hmdp.exception;

/**
 * 分布式锁续期失败异常
 */
public class LockRenewalFailedException extends RuntimeException {
    public LockRenewalFailedException(String message) {
        super(message);
    }
}