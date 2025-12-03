package com.hmdp.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// 泛型类：T 代表业务数据的类型（如 Shop、User 等）
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RedisData<T> {
    private LocalDateTime expireTime; // 逻辑过期时间
    // private Object data;
    private T data; // 泛型业务数据（替代原 Object 类型）
}
