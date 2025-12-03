package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


@Component
public class CacheClient {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public static final Long CACHE_NULL_TTL = 2L;
    public static final String LOCK_PREFIX = "lock:";
    public static final Long LOCK_TTL = 10L;

    // 缓存重建线程池（明确配置，避免任务丢失）
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = new ThreadPoolExecutor(
            5, // 核心线程数（根据业务调整）
            10, // 最大线程数
            60L, // 空闲线程存活时间
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100), // 任务队列（有界，避免内存溢出）
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略：调用者线程执行（避免任务丢失）
    );

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            CACHE_REBUILD_EXECUTOR.shutdown();
        }));
    }

    /**
     * 设置过期时间
     * @param key
     * @param value
     * @param expireTime
     * @param timeUnit
     * @param <T>
     */
    public <T> void set(String key, T value, Long expireTime, TimeUnit timeUnit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), expireTime, timeUnit);
    }

    /**
     * 设置逻辑过期时间
     * @param key
     * @param value
     * @param expireTime
     * @param timeUnit
     * @param <T>
     */
    public <T> void setWithLogicalExpire(String key, T value, Long expireTime, TimeUnit timeUnit) {
        RedisData<T> redisData = new RedisData<>();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(expireTime)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 解决缓存穿透问题
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param timeUnit
     * @return
     * @param <D>
     * @param <ID>
     */
    public <D, ID> D queryWithPassThrough(
            String keyPrefix,
            ID id ,
            Class<D> type,
            Function<ID, D> dbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        // 调用下面的通用方法：用 Lambda 生成 Key（前缀+ID），DTO 直接传 id（复用通用逻辑）
        return queryWithPassThrough(
                (d) -> keyPrefix + d, // d 对应传入的 id（DTO=ID）
                id,
                type,
                (d) -> dbFallback.apply(d), // d 直接是 ID 类型，无需转换
                time,
                timeUnit
        );
    }

    /**
     * 解决缓存穿透问题（支持 DTO 传参 + 自定义 Key 生成）
     * @param keyGenerator 自定义 Key 生成器（从 DTO 中生成 Redis 缓存 Key）
     * @param dto 数据传输对象（入参载体）
     * @param type 业务数据类型（如 Shop.class，用于 JSON 反序列化）
     * @param dbFallback 数据库查询回调（缓存未命中时执行，入参为 DTO）
     * @param time 缓存过期时间（业务数据的过期时间）
     * @param timeUnit 缓存时间单位（业务数据的时间单位）
     * @return 业务数据（D 类型），数据库无数据则返回 null
     * @param <D> 业务数据类型（如 Shop、Order）
     * @param <DTO> 数据传输对象类型（如 ShopQueryDTO）
     */
    public <D, DTO> D queryWithPassThrough(
            Function<DTO, String> keyGenerator,
            DTO dto,
            Class<D> type,
            Function<DTO, D> dbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        // 1. 校验入参合法性（避免空指针）
        if (keyGenerator == null) {
            throw new IllegalArgumentException("Key生成器不能为空");
        }
        if (dto == null) {
            throw new IllegalArgumentException("DTO参数不能为空");
        }
        if (type == null) {
            throw new IllegalArgumentException("业务数据类型不能为空");
        }
        if (dbFallback == null) {
            throw new IllegalArgumentException("数据库查询回调不能为空");
        }

        // 2.生成key
        String key = keyGenerator.apply(dto);
        if (StrUtil.isBlank(key)) {
            throw new IllegalArgumentException("生成的缓存Key不能为空");
        }
        // 3.从redis中查询数据
        String json = stringRedisTemplate.opsForValue().get(key);
        // 4.判断数据是否存在
        if (StrUtil.isNotBlank(json)) {
            try {
                return JSONUtil.toBean(json, type);
            } catch (Exception e) {
                // 反序列化失败：删除损坏的缓存，后续走数据库兜底
                stringRedisTemplate.delete(key);
            }
        }

        // redis内容为空值，说明数据库中不存在该数据
        if (json != null) {
            return null;
        }

        // 4.未命中，查询数据库
        D data = dbFallback.apply(dto);
        // 5.数据库中不存在，写入空值，报错
        if (data == null) {
            // redis中缓存空值
            set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回空值
            return null;
        }
        // 6.数据库有数据：写入缓存并返回
        set(key, data, time, timeUnit);
        return data;
    }

    /**
     * 默认重试次数等于3
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param timeUnit
     * @return
     * @param <D>
     * @param <ID>
     * @implNote 内部默认重试次数为5次
     */
    public <D, ID> D queryWithMutex(
            String keyPrefix,
            ID id,
            Class<D> type,
            Function<ID, D> dbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        // 内部调用私有方法时，传入默认重试次数 5
        return queryWithMutex(keyPrefix, id, type, dbFallback, time, timeUnit, 5);
    }

    /**
     * 互斥锁解决热点key
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param timeUnit
     * @param retryCount
     * @return
     * @param <D>
     * @param <ID>
     */
    public <D, ID> D queryWithMutex(
            String keyPrefix,
            ID id ,
            Class<D> type,
            Function<ID, D> dbFallback,
            Long time,
            TimeUnit timeUnit,
            int retryCount
    ) {
        return queryWithMutex(
                (d) -> keyPrefix + d, // d 对应传入的 id（DTO=ID）
                id,
                type,
                (d) -> dbFallback.apply(d), // d 直接是 ID 类型，无需转换
                time,
                timeUnit,
                retryCount
        );
    }

    /**
     * 互斥锁解决热点key
     * @param keyGenerator
     * @param dto
     * @param type
     * @param dbFallback
     * @param time
     * @param timeUnit
     * @param retryCount
     * @return
     * @param <D>
     * @param <DTO>
     */
    public <D, DTO> D queryWithMutex(
            Function<DTO, String> keyGenerator,
            DTO dto,
            Class<D> type,
            Function<DTO, D> dbFallback,
            Long time,
            TimeUnit timeUnit,
            int retryCount
    ) {
        // 1. 校验入参合法性（避免空指针）
        if (keyGenerator == null) {
            throw new IllegalArgumentException("Key生成器不能为空");
        }
        if (dto == null) {
            throw new IllegalArgumentException("DTO参数不能为空");
        }
        if (type == null) {
            throw new IllegalArgumentException("业务数据类型不能为空");
        }
        if (dbFallback == null) {
            throw new IllegalArgumentException("数据库查询回调不能为空");
        }

        // 2.生成key
        String key = keyGenerator.apply(dto);
        if (StrUtil.isBlank(key)) {
            throw new IllegalArgumentException("生成的缓存Key不能为空");
        }

        String json = stringRedisTemplate.opsForValue().get(key);
        // 3.判断店铺是否存在
        if (StrUtil.isNotBlank(json)) {
            // 若命中，直接返回
            D data = JSONUtil.toBean(json, type);
            return data;
        }

        // redis内容为空值，说明数据库中不存在该数据
        if (json != null) {
            return null;
        }
        String lockKey = LOCK_PREFIX + key;
        String lockValue = UUID.randomUUID().toString(); // 用UUID标识线程
        D data = null;
        // 原子引用一个续期任务的Future（用于后续取消）
        AtomicReference<Future<?>> renewalFutureRef = new AtomicReference<>();

        try {
            // 3.缓存重建
            // 3.1.获取锁
            // 3.2.判断是否获取到锁
            boolean isLock = tryLock(lockKey, lockValue);
            if (!isLock) {
                if (retryCount <= 0) {
                    throw new RuntimeException("获取锁失败，请稍后重试");
                }
                Thread.sleep(50);
                return queryWithMutex(
                        keyGenerator,
                        dto,
                        type,
                        dbFallback,
                        time,
                        timeUnit,
                        retryCount - 1
                );
            }

            // 用set()方法存储续期任务的Future
            renewalFutureRef.set(startLockRenewal(lockKey, lockValue));

            // 3.3.成功，根据id查询数据库
            data = dbFallback.apply(dto);
            // 4.数据库中不存在，写入空值，报错
            if (data == null) {
                // redis中缓存空值
                set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回空值
                return null;
            }
            // 5.存在，写入redis中
            set(key, data, time, timeUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 6.释放锁
            unlock(lockKey, lockValue);
            // 用get()方法获取Future，判断非空后取消
            Future<?> renewalFuture = renewalFutureRef.get();
            if (renewalFuture != null && !renewalFuture.isCancelled()) {
                renewalFuture.cancel(true); // 允许中断正在执行的续期任务
            }
        }

        // 7.返回
        return data;
    }

    /**
     * 逻辑过期，解决热点key问题
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param timeUnit
     * @return
     * @param <D>
     * @param <ID>
     */
    public <D, ID> D queryWithLogicalExpire(
            String keyPrefix,
            ID id ,
            Class<D> type,
            Function<ID, D> dbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        // 调用下面的通用方法：用 Lambda 生成 Key（前缀+ID），DTO 直接传 id（复用通用逻辑）
        return queryWithLogicalExpire(
                (d) -> keyPrefix + d, // d 对应传入的 id（DTO=ID）
                id,
                type,
                (d) -> dbFallback.apply(d), // d 直接是 ID 类型，无需转换
                time,
                timeUnit
        );
    }

    /**
     * 逻辑过期，解决热点key问题
     * @param keyGenerator
     * @param dto
     * @param type
     * @param dbFallback
     * @param time
     * @param timeUnit
     * @return
     * @param <D>
     * @param <DTO>
     */
    public <D, DTO> D queryWithLogicalExpire(
            Function<DTO, String> keyGenerator,
            DTO dto,
            Class<D> type,
            Function<DTO, D> dbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        // 0. 校验入参合法性（避免空指针）
        if (keyGenerator == null) {
            throw new IllegalArgumentException("Key生成器不能为空");
        }
        if (dto == null) {
            throw new IllegalArgumentException("DTO参数不能为空");
        }
        if (type == null) {
            throw new IllegalArgumentException("业务数据类型不能为空");
        }
        if (dbFallback == null) {
            throw new IllegalArgumentException("数据库查询回调不能为空");
        }

        // 1.生成key
        String key = keyGenerator.apply(dto);
        if (StrUtil.isBlank(key)) {
            throw new IllegalArgumentException("生成的缓存Key不能为空");
        }

        // 2.从redis中查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 3.判断数据是否存在，说明未设置逻辑时间
        if (StrUtil.isBlank(json)) {
            D dbData = dbFallback.apply(dto);
            if (dbData != null) {
                setWithLogicalExpire(key, dbData, time, timeUnit);
            }
            return dbData; // 第一次查询直接返回数据库数据，后续查询命中缓存
        }

        // 4.反序列化json
        // 手动构建「RedisData<具体类型>」的Type（如RedisData<Shop>）
        Type redisDataType = buildRedisDataType(type);
        RedisData<D> redisData = deserializeJson(json, redisDataType);
        if (redisData == null || redisData.getData() == null) {
            stringRedisTemplate.delete(key);
            // 缓存数据损坏：查数据库兜底并重建缓存
            D dbData = dbFallback.apply(dto);
            if (dbData != null) {
                setWithLogicalExpire(key, dbData, time, timeUnit);
            }
            return dbData;
        }

        D data = redisData.getData();
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5. 缓存未过期：直接返回
        if (expireTime.isAfter(LocalDateTime.now())) {
            return data;
        }

        // 6.1.过期，获取互斥锁
        String lockKey = LOCK_PREFIX + key;
        String lockValue = UUID.randomUUID().toString(); // 用UUID标识线程
        boolean isLock = tryLock(lockKey, lockValue);
        // 原子引用一个续期任务的Future（用于后续取消）
        AtomicReference<Future<?>> renewalFutureRef = new AtomicReference<>();

        if (isLock) {
            // 用set()方法存储续期任务的Future
            renewalFutureRef.set(startLockRenewal(lockKey, lockValue));

            // 6.2.二次校验缓存（判断其它线程是否已重建，避免重复构建）
            String newJson = stringRedisTemplate.opsForValue().get(key);
            Type newRedisDataType = buildRedisDataType(type); // 二次校验也用同样的Type构建
            RedisData<D> newRedisData = deserializeJson(newJson, newRedisDataType);
            if (newRedisData != null && newRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
                /**        其他线程已重建，返回新数据      **/
                // 5.2.1释放锁
                unlock(lockKey, lockValue);
                // 5.2.2返回新数据
                return newRedisData.getData();
            }

            // 5.3.线程池异步重建缓存
            try {
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    // 5.3.1.重建缓存
                    try {
                        D dbData = dbFallback.apply(dto);
                        if (dbData != null) {
                            this.setWithLogicalExpire(key, dbData, time, timeUnit);
                        }
                    } catch (RuntimeException e) {
                        throw new RuntimeException(e);
                    } finally {
                        // 4.3.2.释放锁
                        unlock(lockKey, lockValue);
                        // 用get()方法获取Future，判断非空后取消
                        Future<?> renewalFuture = renewalFutureRef.get();
                        if (renewalFuture != null && !renewalFuture.isCancelled()) {
                            renewalFuture.cancel(true); // 允许中断正在执行的续期任务
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                // 线程池满：调用者线程同步重建，避免任务丢失
                D dbData = dbFallback.apply(dto);
                if (dbData != null) {
                    setWithLogicalExpire(key, dbData, time, timeUnit);
                }
                unlock(lockKey, lockValue);
                // 用get()方法获取Future，判断非空后取消
                Future<?> renewalFuture = renewalFutureRef.get();
                if (renewalFuture != null && !renewalFuture.isCancelled()) {
                    renewalFuture.cancel(true); // 允许中断正在执行的续期任务
                }
            } catch (Exception e) {
                unlock(lockKey, lockValue); // 异常时释放锁，避免死锁
                // 用get()方法获取Future，判断非空后取消
                Future<?> renewalFuture = renewalFutureRef.get();
                if (renewalFuture != null && !renewalFuture.isCancelled()) {
                    renewalFuture.cancel(true); // 允许中断正在执行的续期任务
                }
                throw new RuntimeException("缓存重建逻辑异常", e);
            }
        }

        // 6.直接返回旧的数据
        return data;
    }

    /**
     * 辅助方法1：手动构建RedisData<具体类型>的Type（突破泛型擦除）
     * 例如：传入Shop.class，返回RedisData<Shop>的Type
     */
    private <D> Type buildRedisDataType(Class<D> innerType) {
        return new ParameterizedType() {
            @Override
            public Type[] getActualTypeArguments() {
                // 泛型实际类型：就是传入的具体实体类（如Shop.class）
                return new Type[]{innerType};
            }

            @Override
            public Type getRawType() {
                // 原始类型：RedisData.class
                return RedisData.class;
            }

            @Override
            public Type getOwnerType() {
                // 没有外部类，返回null
                return null;
            }
        };
    }

    /**
     * 辅助方法2：反序列化JSON（处理空值和异常），接收明确的TypeReference，避免泛型擦除
     */
    private <D> RedisData<D> deserializeJson(String json, Type redisDataType) {
        if (StrUtil.isBlank(json)) {
            return null;
        }
        try {
            return JSONUtil.toBean(
                    json,
                    redisDataType,
                    false
            );
        } catch (Exception e) {
            // 反序列化失败：返回null，触发数据库兜底
            return null;
        }
    }

    /**
     * 线程池锁续期（复用缓存重建线程池，返回Future用于中断）
     */
    private Future<?> startLockRenewal(String lockKey, String lockValue) {
        // 续期任务：循环续期直到被中断或续期失败
        Runnable renewalTask = () -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // 每隔锁TTL的1/3时间续期一次
                    Thread.sleep(LOCK_TTL * 1000L / 3);

                    // 校验锁归属，只续期自己的锁
                    Boolean renewalSuccess = stringRedisTemplate.opsForValue().setIfPresent(
                            lockKey,
                            lockValue,
                            Duration.ofSeconds(LOCK_TTL)
                    );

                    // 续期失败（锁已释放/归属其他线程），终止任务
                    if (BooleanUtil.isFalse(renewalSuccess)) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                // 响应中断（Future.cancel(true)触发），终止续期
                Thread.currentThread().interrupt();
            }
        };

        // 向缓存重建线程池提交续期任务，返回Future用于后续取消
        return CACHE_REBUILD_EXECUTOR.submit(renewalTask);
    }

    /**
     * 获取分布式锁（带唯一value）
     */
    private boolean tryLock(String key, String value) {
        // value取随机值，删除线程时，用于区分线程归属
        Boolean isLock = stringRedisTemplate.opsForValue().setIfAbsent(key, value, Duration.ofSeconds(LOCK_TTL));
        return BooleanUtil.isTrue(isLock);
    }

    /**
     * 释放分布式锁（Lua脚本保证原子性）
     */
    private void unlock(String key, String value) {
        String luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "return redis.call('del', KEYS[1]) " +
                "else " +
                "return 0 " +
                "end";
        stringRedisTemplate.execute(
                new DefaultRedisScript<>(luaScript, Long.class),
                Collections.singletonList(key),
                value
        );
    }
}
