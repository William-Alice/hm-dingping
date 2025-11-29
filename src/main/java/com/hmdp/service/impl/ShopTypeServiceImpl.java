package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletResponse;
import java.util.*;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private HttpServletResponse httpServletResponse;

    /**
     * 查询店铺类型列表
     * @return
     */
    public List<ShopType> queryTypeList() {
        // 1.查询redis
        String key = CACHE_SHOP_TYPE_KEY;
        Set<String> stringSet = stringRedisTemplate.opsForZSet().range(key, 0, -1);
        // 2.判断是否命中
        if (stringSet != null && !stringSet.isEmpty()) {
            // 命中直接返回
            List<String> stringList = new ArrayList<>(stringSet);
            List<ShopType> typeList = new ArrayList<>();
            // 循环逐个解析，避免批量转换的类型问题
            for (String jsonStr : stringList) {
                ShopType shopType = JSONUtil.toBean(jsonStr, ShopType.class);
                typeList.add(shopType);
            }
            return typeList;
        }
        // 3.未命中，查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 4.数据库也不存在，返回404
        if (CollUtil.isEmpty(typeList)) {
            httpServletResponse.setStatus(HttpServletResponse.SC_NOT_FOUND);
            throw new RuntimeException("店铺类型为空");
        }

        // 5.存在，写入redis中
        Set<ZSetOperations.TypedTuple<String>> zSetTuples = new HashSet<>();
        for (ShopType shopType : typeList) {
            try {
                // a. 将ShopType对象转为JSON字符串（作为ZSet的member）
                String shopTypeJson = JSONUtil.toJsonStr(shopType);
                // b. 以sort字段作为score（保持排序一致）
                Double score = (double)shopType.getSort();
                // c. 封装为TypedTuple（包含member和score）
                ZSetOperations.TypedTuple<String> tuple = new ZSetOperations.TypedTuple<String>() {

                    @Override
                    public String getValue() {
                        return shopTypeJson;
                    }

                    @Override
                    public Double getScore() {
                        return score;
                    }

                    @Override
                    public int compareTo(ZSetOperations.TypedTuple<String> o) {
                        return this.getScore().compareTo(o.getScore());
                    }
                };
                zSetTuples.add(tuple);
            } catch (Exception e) {
                // 处理序列化异常（如日志记录）
                throw new RuntimeException(e);
            }
        }
        stringRedisTemplate.opsForZSet().add(key, zSetTuples);
        // 6.返回
        return typeList;
    }
}
