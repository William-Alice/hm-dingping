package com.hmdp.utils;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.DistributedLock;
import com.hmdp.exception.LockAcquireFailedException;
import com.hmdp.mapper.DistributedLockMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class DbDistributedLock {
    // 构造器注入（解决字段注入不推荐问题）
    private final DistributedLockMapper lockMapper;
    private final DistributedLockAcquirer lockAcquirer;

    public DbDistributedLock(DistributedLockMapper lockMapper, DistributedLockAcquirer lockAcquirer) {
        this.lockMapper = lockMapper;
        this.lockAcquirer = lockAcquirer;
    }

    // 锁默认超时时间（防止死锁）
    private static final int LOCK_EXPIRE_SEC = 30;
    // 锁续期间隔（每10秒续期）
    private static final int RENEWAL_INTERVAL_SEC = 10;

    /**
     * 获取分布式锁并执行业务（核心方法）
     * @param lockKey 锁唯一标识（如：seckill:voucher:10:user:1001）
     * @param business 要执行的业务逻辑
     * @return 业务执行结果
     * @throws LockAcquireFailedException 锁获取失败
     * @throws Exception 业务执行异常
     */
    public <T> T executeWithLock(String lockKey, BusinessTask<T> business) throws Exception {
        // 1. 生成唯一持有者标识（防止误释放）
        String holder = UUID.randomUUID().toString();
        LocalDateTime expireTime = LocalDateTime.now().plusSeconds(LOCK_EXPIRE_SEC);
        Thread renewalThread = null; // 初始化续期线程为null，避免空指针

        try {
            // 2. 抢占锁（跨类调用，触发AOP事务）
            lockAcquirer.acquireLock(lockKey, holder, expireTime);

            // 3. 启动锁续期线程（防止业务执行超时）
            renewalThread = startLockRenewal(lockKey, holder);

            // 4. 执行业务逻辑
            return business.run();

        } catch (DuplicateKeyException e) {
            // 唯一键冲突（并发插入锁记录）→ 转为锁获取失败异常
            log.error("并发创建锁记录失败，lockKey={}", lockKey, e);
            throw new LockAcquireFailedException("锁被其他线程抢占");
        } catch (LockAcquireFailedException e) {
            // 主动抛出的锁异常，直接向上传递
            throw e;
        } catch (Exception e) {
            // 业务异常/其他异常，记录日志后向上传递
            log.error("分布式锁执行业务失败，lockKey={}", lockKey, e);
            throw e;
        } finally {
            // 5. 终止续期线程（先判空，避免NPE）
            if (renewalThread != null && !renewalThread.isInterrupted()) {
                renewalThread.interrupt();
                log.debug("锁续期线程已终止，lockKey={}", lockKey);
            }

            // 6. 释放锁（无论业务成功/失败，都释放）
            releaseLock(lockKey, holder);
        }
    }

    /**
     * 释放锁（删除优先，删除失败则强制过期）
     */
    private void releaseLock(String lockKey, String holder) {
        try {
            // 第一步：尝试删除锁记录（精准匹配持有者）
            int deleteCount = lockMapper.delete(new QueryWrapper<DistributedLock>()
                    .eq("lock_key", lockKey)
                    .eq("holder", holder)
            );

            if (deleteCount > 0) {
                log.debug("锁记录删除成功，lockKey={}, holder={}", lockKey, holder);
                return;
            }

            // 第二步：删除失败 → 强制将锁标记为过期（兜底方案）
            int updateCount = lockMapper.update(
                    new DistributedLock() {{
                        setExpireTime(LocalDateTime.now().minusSeconds(1));
                    }},
                    new QueryWrapper<DistributedLock>()
                            .eq("lock_key", lockKey)
                            .eq("holder", holder)
            );

            if (updateCount > 0) {
                log.debug("锁记录强制过期成功，lockKey={}, holder={}", lockKey, holder);
            } else {
                log.warn("锁释放失败（删除/过期均失败），lockKey={}, holder={}", lockKey, holder);
            }
        } catch (Exception e) {
            log.error("锁释放过程异常，lockKey={}, holder={}", lockKey, holder, e);
        }
    }

    /**
     * 启动锁续期线程（防止业务执行超时导致锁过期）
     */
    private Thread startLockRenewal(String lockKey, String holder) {
        Thread renewalThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // 每隔10秒续期一次
                    TimeUnit.SECONDS.sleep(RENEWAL_INTERVAL_SEC);

                    // 延长锁过期时间
                    LocalDateTime newExpire = LocalDateTime.now().plusSeconds(LOCK_EXPIRE_SEC);
                    int updateCount = lockMapper.extendLockExpire(lockKey, holder, newExpire);

                    if (updateCount == 0) {
                        // 续期失败（锁已被释放/抢占）→ 终止线程
                        log.debug("锁续期失败，lockKey={}, holder={}（锁已释放/被抢占）", lockKey, holder);
                        break;
                    }
                    log.debug("锁续期成功，lockKey={}, newExpire={}", lockKey, newExpire);
                }
            } catch (InterruptedException e) {
                // 线程被中断（正常释放锁）→ 优雅退出
                log.debug("锁续期线程被中断，lockKey={}", lockKey);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // 续期异常 → 记录日志并终止线程
                log.error("锁续期线程异常，lockKey={}", lockKey, e);
            }
        }, "lock-renewal-" + lockKey);

        // 设置为守护线程，避免阻塞JVM退出
        renewalThread.setDaemon(true);
        renewalThread.start();
        return renewalThread;
    }

    /**
     * 业务逻辑函数式接口（简化调用）
     */
    @FunctionalInterface
    public interface BusinessTask<T> {
        T run() throws Exception;
    }
}