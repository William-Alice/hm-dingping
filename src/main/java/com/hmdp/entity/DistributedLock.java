package com.hmdp.entity;

// 分布式锁实体类
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("distributed_lock")
public class DistributedLock {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String lockKey;      // 锁唯一标识
    private String holder;       // 锁持有者（UUID）
    private LocalDateTime expireTime; // 锁过期时间
}