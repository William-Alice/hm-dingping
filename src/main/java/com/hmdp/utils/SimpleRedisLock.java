package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private final StringRedisTemplate stringRedisTemplate;

    private final static String LOCK_PREFIX = "lock:";

    private final String key;
    private final String value;

    public SimpleRedisLock (StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate =  stringRedisTemplate;
        this.key = LOCK_PREFIX + name;
        this.value = UUID.randomUUID().toString() + ":" +String.valueOf(Thread.currentThread().getId());
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        Boolean isLock = stringRedisTemplate.opsForValue().setIfAbsent(key, value, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(isLock);
    }

    @Override
    public void unLock() {
        Boolean isUnLock = stringRedisTemplate.delete(key);
    }
}
