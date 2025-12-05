package com.hmdp.utils;

import cn.hutool.core.collection.CollectionUtil;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;


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
     * 批量查询：缓存穿透处理
     * @param keyGenerator
     * @param dtos
     * @param type
     * @param batchDbFallback
     * @param time
     * @param timeUnit
     * @return
     * @param <D>
     * @param <DTO>
     */
    public <D, DTO> Map<DTO, D> batchQueryWithPassThrough(
            Function<DTO, String> keyGenerator,
            List<DTO> dtos,
            Class<D> type,
            Function<List<DTO>, Map<DTO, D>> batchDbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        // 1. 批量查询Redis缓存
        Map<String, D> cacheMap = new HashMap<>();
        Map<DTO, String> dtoKeyMap = new HashMap<>();
        for (DTO dto : dtos) {
            String key = keyGenerator.apply(dto);
            dtoKeyMap.put(dto, key);
            String json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                cacheMap.put(key, JSONUtil.toBean(json, type));
            }
        }

        // 2. 筛选未命中的DTO
        List<DTO> missDtos = dtos.stream()
                .filter(dto -> !cacheMap.containsKey(dtoKeyMap.get(dto)))
                .collect(Collectors.toList());
        if (CollectionUtil.isEmpty(missDtos)) {
            // 全部命中，转换为DTO->D映射
            return dtos.stream().collect(Collectors.toMap(
                    Function.identity(),
                    dto -> cacheMap.get(dtoKeyMap.get(dto))
            ));
        }

        // 3. 批量查询数据库（多SQL聚合逻辑写在batchDbFallback中）
        Map<DTO, D> dbMap = batchDbFallback.apply(missDtos);

        // 4. 批量写入缓存（空值处理）
        for (DTO dto : missDtos) {
            String key = dtoKeyMap.get(dto);
            D data = dbMap.get(dto);
            if (data == null) {
                set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            } else {
                set(key, data, time, timeUnit);
                cacheMap.put(key, data);
            }
        }

        // 5. 组装结果返回
        return dtos.stream().collect(Collectors.toMap(
                Function.identity(),
                dto -> cacheMap.getOrDefault(dtoKeyMap.get(dto), null)
        ));
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
     * 重载方法：默认重试次数5次（简化调用）
     */
    public <D, DTO> Map<DTO, D> batchQueryWithMutex(
            Function<DTO, String> keyGenerator,
            List<DTO> dtos,
            Class<D> type,
            Function<List<DTO>, Map<DTO, D>> batchDbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        return batchQueryWithMutex(keyGenerator, dtos, type, batchDbFallback, time, timeUnit, 5);
    }

    /**
     * 互斥锁策略：批量查询（解决缓存击穿）
     * @param keyGenerator 自定义Key生成器（从DTO生成Redis缓存Key）
     * @param dtos 批量查询的DTO列表
     * @param type 业务数据类型（如Shop.class）
     * @param batchDbFallback 批量查询数据库的回调（入参为未命中缓存的DTO列表）
     * @param time 缓存过期时间
     * @param timeUnit 缓存时间单位
     * @param retryCount 获取锁失败的重试次数
     * @return DTO → 对应业务数据的Map（未命中/无数据则value为null）
     * @param <D> 业务数据类型
     * @param <DTO> 数据传输对象类型
     */
    public <D, DTO> Map<DTO, D> batchQueryWithMutex(
            Function<DTO, String> keyGenerator,
            List<DTO> dtos,
            Class<D> type,
            Function<List<DTO>, Map<DTO, D>> batchDbFallback,
            Long time,
            TimeUnit timeUnit,
            int retryCount
    ) {
        // 1. 入参合法性校验
        if (keyGenerator == null) {
            throw new IllegalArgumentException("Key生成器不能为空");
        }
        if (CollectionUtil.isEmpty(dtos)) {
            throw new IllegalArgumentException("批量查询的DTO列表不能为空");
        }
        if (type == null) {
            throw new IllegalArgumentException("业务数据类型不能为空");
        }
        if (batchDbFallback == null) {
            throw new IllegalArgumentException("数据库批量查询回调不能为空");
        }
        if (time == null || time <= 0) {
            throw new IllegalArgumentException("缓存过期时间必须大于0");
        }
        if (timeUnit == null) {
            throw new IllegalArgumentException("缓存时间单位不能为空");
        }

        // 2. 批量查询Redis缓存，构建DTO→Key/缓存数据映射
        Map<String, D> cacheMap = new HashMap<>();
        Map<DTO, String> dtoKeyMap = new HashMap<>();
        for (DTO dto : dtos) {
            String cacheKey = keyGenerator.apply(dto);
            if (StrUtil.isBlank(cacheKey)) {
                throw new IllegalArgumentException("DTO生成的缓存Key不能为空：" + dto);
            }
            dtoKeyMap.put(dto, cacheKey);
            String json = stringRedisTemplate.opsForValue().get(cacheKey);
            if (StrUtil.isNotBlank(json)) {
                try {
                    cacheMap.put(cacheKey, JSONUtil.toBean(json, type));
                } catch (Exception e) {
                    // 反序列化失败：删除损坏缓存，后续走数据库兜底
                    stringRedisTemplate.delete(cacheKey);
                }
            } else if (json != null) {
                // 缓存空值（""），直接标记为null，避免穿透
                cacheMap.put(cacheKey, null);
            }
        }

        // 3. 筛选未命中缓存的DTO（缓存无数据/反序列化失败）
        List<DTO> missDtos = dtos.stream()
                .filter(dto -> !cacheMap.containsKey(dtoKeyMap.get(dto)))
                .collect(Collectors.toList());
        if (CollectionUtil.isEmpty(missDtos)) {
            // 全部命中，直接组装结果返回
            return dtos.stream().collect(Collectors.toMap(
                    Function.identity(),
                    dto -> cacheMap.get(dtoKeyMap.get(dto)),
                    (oldValue, newValue) -> oldValue, // 处理重复DTO
                    LinkedHashMap::new // 保留入参顺序
            ));
        }

        // 4. 构建未命中DTO的锁映射（每个DTO对应独立锁，避免批量锁阻塞）
        Map<DTO, String> dtoLockKeyMap = new HashMap<>();
        Map<DTO, String> dtoLockValueMap = new HashMap<>();
        Map<DTO, Future<?>> renewalFutureMap = new HashMap<>();
        for (DTO dto : missDtos) {
            String cacheKey = dtoKeyMap.get(dto);
            dtoLockKeyMap.put(dto, LOCK_PREFIX + cacheKey);
            dtoLockValueMap.put(dto, UUID.randomUUID().toString());
        }

        // 5. 批量获取互斥锁（失败则重试，释放已获取的锁）
        boolean isAllLockSuccess = false;
        try {
            while (retryCount >= 0) {
                isAllLockSuccess = true;
                // 逐个获取锁，全部成功才继续
                for (DTO dto : missDtos) {
                    String lockKey = dtoLockKeyMap.get(dto);
                    String lockValue = dtoLockValueMap.get(dto);
                    boolean isLock = tryLock(lockKey, lockValue);
                    if (!isLock) {
                        isAllLockSuccess = false;
                        // 释放已获取的锁，避免部分锁残留
                        releaseBatchLock(missDtos, dtoLockKeyMap, dtoLockValueMap, renewalFutureMap);
                        retryCount--;
                        if (retryCount < 0) {
                            throw new RuntimeException("批量获取锁失败，重试次数用尽：剩余重试次数=" + retryCount);
                        }
                        Thread.sleep(50); // 重试间隔50ms，降低并发冲突
                        break;
                    }
                    // 获取锁成功，启动锁续期任务
                    Future<?> renewalFuture = startLockRenewal(lockKey, lockValue);
                    renewalFutureMap.put(dto, renewalFuture);
                }
                if (isAllLockSuccess) {
                    break;
                }
            }

            // 6. 双重检查缓存（避免加锁期间其他线程已写入缓存）
            refreshCacheMap(missDtos, dtoKeyMap, cacheMap, type);

            // 7. 筛选仍未命中的DTO（双重检查后仍无数据）
            List<DTO> finalMissDtos = missDtos.stream()
                    .filter(dto -> !cacheMap.containsKey(dtoKeyMap.get(dto)))
                    .collect(Collectors.toList());

            // 8. 批量查询数据库（仅查询最终未命中的DTO）
            Map<DTO, D> dbResultMap = new HashMap<>();
            if (!CollectionUtil.isEmpty(finalMissDtos)) {
                dbResultMap = batchDbFallback.apply(finalMissDtos);
                // 9. 批量写入缓存（包括空值处理，避免缓存穿透）
                for (DTO dto : finalMissDtos) {
                    String cacheKey = dtoKeyMap.get(dto);
                    D data = dbResultMap.getOrDefault(dto, null);
                    if (data == null) {
                        set(cacheKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                        cacheMap.put(cacheKey, null);
                    } else {
                        set(cacheKey, data, time, timeUnit);
                        cacheMap.put(cacheKey, data);
                    }
                }
            }

            // 10. 组装最终结果返回（保留入参顺序）
            return dtos.stream().collect(Collectors.toMap(
                    Function.identity(),
                    dto -> cacheMap.getOrDefault(dtoKeyMap.get(dto), null),
                    (oldValue, newValue) -> oldValue,
                    LinkedHashMap::new
            ));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("批量查询获取锁时线程中断", e);
        } finally {
            // 11. 释放所有锁 + 取消续期任务（无论成功/失败，必须释放）
            releaseBatchLock(missDtos, dtoLockKeyMap, dtoLockValueMap, renewalFutureMap);
        }
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
     * 逻辑过期策略：批量查询（解决批量热点key）
     * @param keyGenerator 自定义Key生成器（从DTO生成Redis缓存Key）
     * @param dtos 批量查询的DTO列表
     * @param type 业务数据类型（如Shop.class）
     * @param batchDbFallback 批量查询数据库的回调（入参为需重建缓存的DTO列表）
     * @param time 逻辑过期时间（缓存重建后新的过期时长）
     * @param timeUnit 时间单位
     * @return DTO → 对应业务数据的Map（保留入参顺序，过期返回旧数据）
     * @param <D> 业务数据类型
     * @param <DTO> 数据传输对象类型
     */
    public <D, DTO> Map<DTO, D> batchQueryWithLogicalExpire(
            Function<DTO, String> keyGenerator,
            List<DTO> dtos,
            Class<D> type,
            Function<List<DTO>, Map<DTO, D>> batchDbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        // 1. 入参合法性校验
        if (keyGenerator == null) {
            throw new IllegalArgumentException("Key生成器不能为空");
        }
        if (CollectionUtil.isEmpty(dtos)) {
            throw new IllegalArgumentException("批量查询的DTO列表不能为空");
        }
        if (type == null) {
            throw new IllegalArgumentException("业务数据类型不能为空");
        }
        if (batchDbFallback == null) {
            throw new IllegalArgumentException("数据库批量查询回调不能为空");
        }
        if (time == null || time <= 0) {
            throw new IllegalArgumentException("逻辑过期时间必须大于0");
        }
        if (timeUnit == null) {
            throw new IllegalArgumentException("时间单位不能为空");
        }

        // 2. 构建核心映射关系
        Map<DTO, String> dtoKeyMap = new HashMap<>(); // DTO→缓存Key
        Map<DTO, RedisData<D>> dtoRedisDataMap = new HashMap<>(); // DTO→RedisData
        Map<DTO, D> resultMap = new LinkedHashMap<>(); // 最终结果（保留入参顺序）
        List<DTO> missDtos = new ArrayList<>(); // 缓存未命中的DTO
        List<DTO> expiredDtos = new ArrayList<>(); // 缓存已过期的DTO

        // 3. 批量查询Redis缓存，解析逻辑过期数据
        for (DTO dto : dtos) {
            String cacheKey = keyGenerator.apply(dto);
            if (StrUtil.isBlank(cacheKey)) {
                throw new IllegalArgumentException("DTO生成的缓存Key不能为空：" + dto);
            }
            dtoKeyMap.put(dto, cacheKey);

            String json = stringRedisTemplate.opsForValue().get(cacheKey);
            if (StrUtil.isBlank(json)) {
                // 缓存未命中，加入missDtos
                missDtos.add(dto);
                continue;
            }

            // 解析RedisData（带逻辑过期时间）
            Type redisDataType = buildRedisDataType(type);
            RedisData<D> redisData = deserializeJson(json, redisDataType);
            if (redisData == null || redisData.getData() == null) {
                // 反序列化失败/数据为空，加入missDtos
                missDtos.add(dto);
                stringRedisTemplate.delete(cacheKey); // 删除损坏缓存
                continue;
            }
            dtoRedisDataMap.put(dto, redisData);

            // 判断逻辑过期状态
            LocalDateTime expireTime = redisData.getExpireTime();
            if (expireTime.isAfter(LocalDateTime.now())) {
                // 未过期，直接返回数据
                resultMap.put(dto, redisData.getData());
            } else {
                // 已过期，加入expiredDtos，先返回旧数据
                resultMap.put(dto, redisData.getData());
                expiredDtos.add(dto);
            }
        }

        // 4. 处理缓存未命中的DTO：同步查库 + 写入逻辑过期缓存
        if (!CollectionUtil.isEmpty(missDtos)) {
            Map<DTO, D> missDbMap = batchDbFallback.apply(missDtos);
            for (DTO dto : missDtos) {
                D data = missDbMap.getOrDefault(dto, null);
                resultMap.put(dto, data);
                if (data != null) {
                    String cacheKey = dtoKeyMap.get(dto);
                    setWithLogicalExpire(cacheKey, data, time, timeUnit);
                }
            }
        }

        // 5. 处理缓存已过期的DTO：批量加锁 + 异步重建缓存
        if (!CollectionUtil.isEmpty(expiredDtos)) {
            rebuildExpiredBatchCache(expiredDtos, dtoKeyMap, type, batchDbFallback, time, timeUnit);
        }

        return resultMap;
    }

    /**
     * 辅助方法：手动构建RedisData<具体类型>的Type（突破泛型擦除）
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
     * 辅助方法：反序列化JSON（处理空值和异常），接收明确的TypeReference，避免泛型擦除
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

    /**
     * 辅助方法：双重检查缓存，刷新缓存Map
     */
    private <D, DTO> void refreshCacheMap(
            List<DTO> missDtos,
            Map<DTO, String> dtoKeyMap,
            Map<String, D> cacheMap,
            Class<D> type
    ) {
        for (DTO dto : missDtos) {
            String cacheKey = dtoKeyMap.get(dto);
            String json = stringRedisTemplate.opsForValue().get(cacheKey);
            if (StrUtil.isNotBlank(json)) {
                try {
                    cacheMap.put(cacheKey, JSONUtil.toBean(json, type));
                } catch (Exception e) {
                    stringRedisTemplate.delete(cacheKey);
                }
            } else if (json != null) {
                cacheMap.put(cacheKey, null);
            }
        }
    }

    /**
     * 辅助方法：释放批量锁 + 取消续期任务
     */
    private <DTO> void releaseBatchLock(
            List<DTO> dtos,
            Map<DTO, String> dtoLockKeyMap,
            Map<DTO, String> dtoLockValueMap,
            Map<DTO, Future<?>> renewalFutureMap
    ) {
        for (DTO dto : dtos) {
            // 取消续期任务
            Future<?> renewalFuture = renewalFutureMap.get(dto);
            if (renewalFuture != null && !renewalFuture.isCancelled()) {
                renewalFuture.cancel(true);
            }
            // 释放锁
            String lockKey = dtoLockKeyMap.get(dto);
            String lockValue = dtoLockValueMap.get(dto);
            if (StrUtil.isNotBlank(lockKey) && StrUtil.isNotBlank(lockValue)) {
                unlock(lockKey, lockValue);
            }
        }
        renewalFutureMap.clear();
    }

    /**
     * 辅助方法：批量重建过期缓存（加互斥锁 + 异步线程池）
     */
    private <D, DTO> void rebuildExpiredBatchCache(
            List<DTO> expiredDtos,
            Map<DTO, String> dtoKeyMap,
            Class<D> type,
            Function<List<DTO>, Map<DTO, D>> batchDbFallback,
            Long time,
            TimeUnit timeUnit
    ) {
        // 构建锁映射：DTO→锁Key/锁Value/续期Future
        Map<DTO, String> dtoLockKeyMap = new HashMap<>();
        Map<DTO, String> dtoLockValueMap = new HashMap<>();
        Map<DTO, Future<?>> renewalFutureMap = new HashMap<>();

        // 批量尝试获取锁（失败则跳过，保留旧数据即可）
        for (DTO dto : expiredDtos) {
            String cacheKey = dtoKeyMap.get(dto);
            String lockKey = LOCK_PREFIX + cacheKey;
            String lockValue = UUID.randomUUID().toString();
            dtoLockKeyMap.put(dto, lockKey);
            dtoLockValueMap.put(dto, lockValue);

            boolean isLock = tryLock(lockKey, lockValue);
            if (isLock) {
                // 获取锁成功，启动续期任务
                Future<?> renewalFuture = startLockRenewal(lockKey, lockValue);
                renewalFutureMap.put(dto, renewalFuture);
            }
        }

        // 筛选成功获取锁的DTO（仅这些DTO需要异步重建）
        List<DTO> lockSuccessDtos = expiredDtos.stream()
                .filter(dto -> renewalFutureMap.containsKey(dto))
                .collect(Collectors.toList());
        if (CollectionUtil.isEmpty(lockSuccessDtos)) {
            return;
        }

        // 异步线程池重建缓存
        try {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 双重检查缓存（避免其他线程已重建）
                    List<DTO> finalRebuildDtos = new ArrayList<>();
                    for (DTO dto : lockSuccessDtos) {
                        String cacheKey = dtoKeyMap.get(dto);
                        String json = stringRedisTemplate.opsForValue().get(cacheKey);
                        Type redisDataType = buildRedisDataType(type);
                        RedisData<D> redisData = deserializeJson(json, redisDataType);
                        if (redisData == null || redisData.getExpireTime().isBefore(LocalDateTime.now())) {
                            finalRebuildDtos.add(dto);
                        }
                    }

                    // 批量查询数据库
                    if (!CollectionUtil.isEmpty(finalRebuildDtos)) {
                        Map<DTO, D> dbMap = batchDbFallback.apply(finalRebuildDtos);
                        // 批量写入逻辑过期缓存
                        for (DTO dto : finalRebuildDtos) {
                            D data = dbMap.getOrDefault(dto, null);
                            if (data != null) {
                                String cacheKey = dtoKeyMap.get(dto);
                                setWithLogicalExpire(cacheKey, data, time, timeUnit);
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("批量重建过期缓存异常", e);
                } finally {
                    // 释放所有锁 + 取消续期任务
                    for (DTO dto : lockSuccessDtos) {
                        // 取消续期任务
                        Future<?> renewalFuture = renewalFutureMap.get(dto);
                        if (renewalFuture != null && !renewalFuture.isCancelled()) {
                            renewalFuture.cancel(true);
                        }
                        // 释放锁
                        String lockKey = dtoLockKeyMap.get(dto);
                        String lockValue = dtoLockValueMap.get(dto);
                        unlock(lockKey, lockValue);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            // 线程池满：调用者线程同步重建（避免缓存重建失败）
            for (DTO dto : lockSuccessDtos) {
                try {
                    String cacheKey = dtoKeyMap.get(dto);
                    D data = batchDbFallback.apply(Collections.singletonList(dto)).get(dto);
                    if (data != null) {
                        setWithLogicalExpire(cacheKey, data, time, timeUnit);
                    }
                } finally {
                    // 释放锁 + 取消续期
                    Future<?> renewalFuture = renewalFutureMap.get(dto);
                    if (renewalFuture != null && !renewalFuture.isCancelled()) {
                        renewalFuture.cancel(true);
                    }
                    String lockKey = dtoLockKeyMap.get(dto);
                    String lockValue = dtoLockValueMap.get(dto);
                    unlock(lockKey, lockValue);
                }
            }
        }
    }
}
