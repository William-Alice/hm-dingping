package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletResponse;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    // 创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 重试次数限制
    private static final int MAX_RETRY = 30;

    /**
     * 根据id查询店铺信息
     * @param id
     * @return
     */
    public Shop queryShopById(Long id) {
        // 缓存穿透
//        return queryWithPassThrough(id);
        // 缓存击穿（互斥锁）
//        return queryWithMutex(id, 1); // 初始重试次数1
        // 缓存击穿（逻辑过期）
        return queryWithLogicalExpire(id); // 初始重试次数1
    }

    /**
     * 逻辑过期，解决热点key问题
     * @param id
     * @return
     */
    private Shop queryWithLogicalExpire(Long id) {
        // 1.从redis中查询店铺
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.判断店铺是否存在
        if (StrUtil.isBlank(shopJson)) {
            // 若未命中，直接返回null
            return null;
        }

        // 3.反序列化shopJson,获取shop对象和过期时间
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 4.判断是否过期
        if (expireTime.isBefore(LocalDateTime.now())) {
            // 4.1.过期，获取互斥锁
            String lockKey = LOCK_SHOP_KEY + id;
            String lockValue = UUID.randomUUID().toString(); // 用UUID标识线程
            boolean isLock = tryLock(lockKey, lockValue);

            if (isLock) {
                // 4.2.再次检测redis缓存是否过期（判断其它线程是否已重建，避免重复构建）
                String newShopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
                RedisData newRedisData = JSONUtil.toBean(newShopJson, RedisData.class);
                if (newRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
                    // 4.2.1释放锁
                    unlock(lockKey, lockValue);
                    // 4.2.2返回新数据
                    Shop newShop = JSONUtil.toBean((JSONObject) newRedisData.getData(), Shop.class);
                    return newShop;
                }

                // 4.3.线程池异步重建缓存
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    // 4.3.1.重建缓存
                    try {
                        this.saveShop2Redis(id, 20L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        // 4.3.2.释放锁
                        unlock(lockKey, lockValue);
                    }
                });
            }
        }
        // 4.2.直接返回旧的shop数据
        return shop;
    }

    /**
     * 互斥锁解决热点key
     * @param id
     * @param retryCount
     * @return
     */
    private Shop queryWithMutex(Long id, int retryCount) {
        // 1.从redis中查询店铺
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断店铺是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 若命中，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        // redis内容不为空，说明数据库中不存在该数据
        if (shopJson != null) {
            throw new RuntimeException("店铺不存在");
        }
        String lockKey = LOCK_SHOP_KEY + id;
        String lockValue = UUID.randomUUID().toString(); // 用UUID标识线程
        Shop shop = null;

        try {
            // 3.缓存重建
            // 3.1.获取锁
            // 3.2.判断是否获取到锁
            boolean isLock = tryLock(lockKey, lockValue);
            if (!isLock) {
                if (retryCount >= MAX_RETRY) {
                    throw new RuntimeException("获取锁失败，请稍后重试");
                }
                Thread.sleep(50);
                return queryWithMutex(id, retryCount + 1);
            }
            // 3.3.成功，根据id查询数据库
            shop = getById(id);
            // 模拟重建的延迟
            Thread.sleep(200);
            // 4.数据库中不存在，写入空值，报错
            if (shop == null) {
                // redis中缓存空值
                stringRedisTemplate.opsForValue().set(key, "", Duration.ofMinutes(CACHE_NULL_TTL));
                // 报错
                throw new RuntimeException("店铺不存在");
            }
            // 5.存在，写入redis中
            String jsonStr = JSONUtil.toJsonStr(shop);
            stringRedisTemplate.opsForValue().set(key, jsonStr, Duration.ofMinutes(CACHE_SHOP_TTL));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 6.释放锁
            unlock(lockKey, lockValue);
        }

        // 7.返回
        return shop;
    }

    /**
     * 常规方案，不设置逻辑过期时间和加锁
     * @param id
     * @return
     */
    private Shop queryWithPassThrough(Long id) {
        // 1.从redis中查询店铺
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.判断店铺是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 若命中，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        // redis内容不为空，说明数据库中不存在该数据
        if (shopJson != null) {
            throw new RuntimeException("店铺不存在");
        }

        // 3.未命中，查询数据库
        Shop shop = getById(id);
        // 4.数据库中不存在，写入空值，报错
        if (shop == null) {
            // redis中缓存空值
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", Duration.ofMinutes(CACHE_NULL_TTL));
            // 报错
            throw new RuntimeException("店铺不存在");
        }
        // 5.存在，写入redis中
        String jsonStr = JSONUtil.toJsonStr(shop);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr, Duration.ofMinutes(CACHE_SHOP_TTL));
        // 6.返回
        return shop;
    }

    /**
     * 创建热点key数据（可过期）
     */
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(100);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入到缓存中
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 获取分布式锁（带唯一value）
     */
    private boolean tryLock(String key, String value) {
        // value取随机值，删除线程时，用于区分线程归属
        Boolean isLock = stringRedisTemplate.opsForValue().setIfAbsent(key, value, Duration.ofSeconds(LOCK_SHOP_TTL));
        return BooleanUtil.isTrue(isLock);
    }

    /**
     * 释放分布式锁（Lua脚本保证原子性）
     */
    private void unlock(String key, String value) {
        // 可能误删其它线程
//        stringRedisTemplate.delete(key);
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
     * 更新店铺信息
     * @param shop
     */
    @Transactional
    public void update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            throw new RuntimeException("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.redis中删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
    }
}
