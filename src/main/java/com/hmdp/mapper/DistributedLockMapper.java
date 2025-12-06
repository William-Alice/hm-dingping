package com.hmdp.mapper;

// 锁Mapper接口
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.DistributedLock;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public interface DistributedLockMapper extends BaseMapper<DistributedLock> {
    // 加排他锁获取锁（同一lock_key会排队）
    @Select("SELECT * FROM distributed_lock WHERE lock_key = #{lockKey} FOR UPDATE")
    DistributedLock getLockWithExLock(@Param("lockKey") String lockKey);

    /**
     * 延长锁过期时间
     */
    @Update("UPDATE distributed_lock SET expire_time = #{newExpire} WHERE lock_key = #{lockKey} AND holder = #{holder}")
    int extendLockExpire(
            @Param("lockKey") String lockKey,
            @Param("holder") String holder,
            @Param("newExpire") LocalDateTime newExpire
    );
}