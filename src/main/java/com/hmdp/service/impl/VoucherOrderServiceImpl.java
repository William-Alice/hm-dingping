package com.hmdp.service.impl;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.DbDistributedLock;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private SeckillVoucherServiceImpl seckillVoucherServiceImpl;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private IVoucherOrderService thisProxy;
    @Autowired
    private DbDistributedLock dbDistributedLock;

    /**
     * 下单秒杀优惠券
     * @param voucherId
     * @return
     */
    public Long seckillVoucher(Long voucherId) throws Exception {
        // 1.查询秒杀优惠券
        SeckillVoucher seckillVoucher = seckillVoucherServiceImpl.getById(voucherId);
        // 2.判断是否不在下单时间内
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())
                ||  seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            throw new RuntimeException("当前不在抢购优惠券的时间内！");
        }

        /*// 3.判断库存是否充足
        if (seckillVoucher.getStock() < 1) {
            throw new RuntimeException("库存不足！");
        }*/

        Long userId = UserHolder.getUser().getId();
        /*synchronized((userId.toString().intern())) {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return thisProxy.createVoucherOrder(voucherId);
        }*/
        // 用数据库分布式锁执行秒杀（锁key：业务+资源ID）
        return dbDistributedLock.executeWithLock(
                "seckill:voucher:" + voucherId + "user:id" + userId,
                () -> thisProxy.createVoucherOrder(voucherId) // 秒杀业务逻辑
        );
    }

    @Transactional
    public Long createVoucherOrder(Long voucherId) {
        // 4.一人一单
        Long userId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            // 用户已经购买过了
            throw new RuntimeException("用户已经购买过一次！");
        }

        // 5.扣减库存
        boolean success = seckillVoucherServiceImpl.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0) // 乐观锁解决超卖问题
                .update();
        if (!success) {
            // 扣减失败
            throw new RuntimeException("扣减失败");
        }

        // 6.构建下单条件
        VoucherOrder voucherOrder = new VoucherOrder();
        // 6.1.订单ID
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 6.2.用户ID
        voucherOrder.setUserId(userId);
        // 6.3.代金券ID
        voucherOrder.setVoucherId(voucherId);
        // 创建订单
        save(voucherOrder);

        // 7.返回订单ID
        return orderId;
    }
}
