package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.DistributedLock;
import com.hmdp.exception.LockAcquireFailedException;
import com.hmdp.mapper.DistributedLockMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Component
@Slf4j
public class DistributedLockAcquirer {
    private final DistributedLockMapper lockMapper;

    // 构造器注入（替代字段注入）
    public DistributedLockAcquirer(DistributedLockMapper lockMapper) {
        this.lockMapper = lockMapper;
    }

    /**
     * 抢占锁（独立类+独立事务，保证AOP生效）
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void acquireLock(String lockKey, String holder, LocalDateTime expireTime) {
        // 加行级排他锁查询，保证同一lockKey互斥
        DistributedLock lock = lockMapper.getLockWithExLock(lockKey);

        if (lock == null) {
            // 锁记录不存在 → 创建新锁
            lock = new DistributedLock();
            lock.setLockKey(lockKey);
            lock.setHolder(holder);
            lock.setExpireTime(expireTime);
            lockMapper.insert(lock);
            log.debug("创建新锁成功，lockKey={}, holder={}", lockKey, holder);
        } else {
            // 锁记录存在 → 检查是否过期
            if (lock.getExpireTime().isBefore(LocalDateTime.now())) {
                // 锁已过期 → 强制抢占
                int updateCount = lockMapper.update(
                        new DistributedLock() {{
                            setHolder(holder);
                            setExpireTime(expireTime);
                        }},
                        new QueryWrapper<DistributedLock>()
                                .eq("lock_key", lockKey)
                                .le("expire_time", LocalDateTime.now())
                );

                if (updateCount == 0) {
                    throw new LockAcquireFailedException("锁被其他线程抢占");
                }
                log.debug("抢占过期锁成功，lockKey={}, holder={}", lockKey, holder);
            } else {
                throw new LockAcquireFailedException("锁已被持有");
            }
        }
    }
}