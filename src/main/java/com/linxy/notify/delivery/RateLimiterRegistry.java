package com.linxy.notify.delivery;

import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * 每供应商独立的令牌桶限速器（对齐 mockingbird BucketQuotaService 的"按桶限流"）。
 * 桶在首次使用时按供应商定义创建；供应商热更新后按新参数重建。
 * 单机内存实现 —— 多实例部署时换 Redis 令牌桶即可（接口不变）。
 */
@Component
public class RateLimiterRegistry {

    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    /** 尝试获取一个配额；false = 当前限速窗口已满，通知保持 RETRYING 稍后再试 */
    public boolean tryAcquire(String supplier, Integer qps, Integer burst) {
        if (qps == null || qps <= 0) return true; // 未配置限速
        TokenBucket bucket = buckets.computeIfAbsent(supplier + ":" + qps + ":" + burst,
                k -> new TokenBucket(qps, burst == null ? qps : burst));
        return bucket.tryAcquire();
    }

    /** 简单令牌桶：稳态 qps，容量 burst */
    static final class TokenBucket {
        private final double capacity;
        private final double refillPerSec;
        private double tokens;
        private long lastRefillNanos;

        TokenBucket(int qps, int burst) {
            this.capacity = burst;
            this.refillPerSec = qps;
            this.tokens = burst;
            this.lastRefillNanos = System.nanoTime();
        }

        synchronized boolean tryAcquire() {
            long now = System.nanoTime();
            double elapsed = (now - lastRefillNanos) / 1e9;
            tokens = Math.min(capacity, tokens + elapsed * refillPerSec);
            lastRefillNanos = now;
            if (tokens >= 1) {
                tokens -= 1;
                return true;
            }
            return false;
        }
    }
}
