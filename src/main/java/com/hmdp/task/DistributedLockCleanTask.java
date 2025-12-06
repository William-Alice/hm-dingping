package com.hmdp.task;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.DistributedLock;
import com.hmdp.mapper.DistributedLockMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * 分布式锁过期记录定时清理任务
 * 清理规则：删除expire_time早于7天前的锁记录
 */
@Component
@Slf4j
public class DistributedLockCleanTask {

    // 构造器注入（符合Spring最佳实践，替代@Autowired字段注入）
    private final DistributedLockMapper lockMapper;

    public DistributedLockCleanTask(DistributedLockMapper lockMapper) {
        this.lockMapper = lockMapper;
    }

    /**
     * 定时清理逻辑：每天凌晨0点执行（避免业务高峰期）
     * cron表达式说明：秒 分 时 日 月 周 → 0 0 0 * * ? 表示每天0点0分0秒执行
     */
    @Scheduled(cron = "0 0 0 * * ?")
    @Transactional(rollbackFor = Exception.class) // 事务保证删除操作原子性
    public void cleanExpiredLock() {
        try {
            // 1. 计算7天前的时间（当前时间 - 7天）
            LocalDateTime sevenDaysAgo = LocalDateTime.now().minusDays(7);

            // 2. 构建查询条件：删除expire_time < 7天前的记录
            QueryWrapper<DistributedLock> queryWrapper = new QueryWrapper<DistributedLock>()
                    .lt("expire_time", sevenDaysAgo);

            // 3. 执行删除并统计删除数量（便于监控）
            int deleteCount = lockMapper.delete(queryWrapper);

            // 4. 日志记录（关键指标：删除数量、执行时间）
            if (deleteCount > 0) {
                log.info("分布式锁过期记录清理成功，清理过期7天以上的记录数：{}，清理时间阈值：{}",
                        deleteCount, sevenDaysAgo);
            } else {
                log.debug("分布式锁过期记录清理完成，无过期7天以上的锁记录，清理时间阈值：{}", sevenDaysAgo);
            }

        } catch (Exception e) {
            // 异常兜底：记录错误日志，避免定时任务中断
            log.error("分布式锁过期记录清理任务执行失败", e);
        }
    }
}