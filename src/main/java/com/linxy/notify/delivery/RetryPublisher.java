package com.linxy.notify.delivery;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linxy.notify.metrics.Metrics;

/**
 * 延迟重试发布器：把通知 ID 发到延迟队列，_AMQ_SCHED_DELIVERY 控制投递时间
 *（对标 RocketMQ delayLevel —— 消费线程不等待，broker 定时再投）。
 * 独立成类以切断 DeliveryEngine ↔ DispatchConsumer 的构造环。
 */
@Component
public class RetryPublisher {

    private static final Logger log = LoggerFactory.getLogger(RetryPublisher.class);
    static final String SCHED_DELIVERY = "_AMQ_SCHED_DELIVERY";   // Artemis 延迟投递属性（毫秒时间戳）
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JmsTemplate jms;
    private final Metrics metrics;
    private final String retryQueue;

    public RetryPublisher(JmsTemplate jms,
                          Metrics metrics,
                          @Value("${notify.dispatch.queue:notify.dispatch}") String dispatchQueue) {
        this.jms = jms;
        this.metrics = metrics;
        this.retryQueue = dispatchQueue + ".retry";
    }

    /** 按延迟毫秒发延迟消息；失败不抛 —— 扫描器会兜底（DB 是事实源） */
    public void publishRetry(String id, long delayMs) {
        String body;
        try {
            body = MAPPER.writeValueAsString(Map.of("id", id));
        } catch (Exception e) {
            log.warn("retry serialize failed id={}", id, e);
            return;
        }
        final String json = body;
        try {
            jms.send(retryQueue, s -> {
                jakarta.jms.TextMessage m = s.createTextMessage(json);
                m.setLongProperty(SCHED_DELIVERY, System.currentTimeMillis() + delayMs);
                return m;
            });
            log.debug("retry scheduled via MQ id={} delayMs={}", id, delayMs);
        } catch (Exception e) {
            log.warn("retry publish failed (sweeper will backstop), id={}", id, e);
            metrics.deliveryCounter("retry_publish_failed");
        }
    }
}
