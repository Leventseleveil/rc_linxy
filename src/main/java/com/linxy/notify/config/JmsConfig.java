package com.linxy.notify.config;

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

/**
 * JMS / 内嵌 Artemis 配置。
 *
 * mode=embedded（默认）：本类生效，进程内启动 Artemis broker（vm://0）。
 *   开启地址/队列自动创建（内嵌演示态可接受；外部集群生产环境由 broker 侧
 *   预建队列并关闭 autoCreate，队列清单在 README 维护）。
 * mode=native（生产）：本类整体失效，连接外部集群（broker-url=tcp://...），
 *   业务代码零改动。
 *
 * 为什么用 Artemis 而不是裸内存队列：完整 JMS 语义 —— 延迟投递
 * （_AMQ_SCHED_DELIVERY，对标 RocketMQ delayLevel）、多队列寻址、消费者并发，
 * 与 RocketMQ/Kafka 同构，不出现"测试/生产双实现"。
 */
@Configuration
@EnableJms
@ConditionalOnProperty(name = "spring.artemis.mode", havingValue = "embedded", matchIfMissing = true)
public class JmsConfig {

    /**
     * 内嵌 broker：vm://0 进程内寻址，非持久（演示态；生产用外部集群持久化）。
     * 静态单例 + 引用计数：同一 JVM 多个 Spring context（如测试）依次启停时，
     * broker 只在首次创建、最后一个 context 关闭时销毁，避免互相踩踏。
     */
    @Bean(destroyMethod = "release")
    public SharedEmbeddedBroker sharedEmbeddedBroker() throws Exception {
        return SharedEmbeddedBroker.acquire();
    }

    /** 消息体用 JSON 文本；无 _type 头时按纯文本处理（业务方直接发 TextMessage 即可） */
    @Bean
    public MessageConverter textFriendlyMessageConverter() {
        return new org.springframework.jms.support.converter.SimpleMessageConverter() {
            @Override
            public Object fromMessage(jakarta.jms.Message message) throws jakarta.jms.JMSException {
                if (message instanceof jakarta.jms.TextMessage tm) {
                    return tm.getText();
                }
                return super.fromMessage(message);
            }
        };
    }

    /** JVM 级共享的内嵌 broker（vm://0），引用计数管理生命周期 */
    static final class SharedEmbeddedBroker {

        private static SharedEmbeddedBroker instance;
        private final EmbeddedActiveMQ broker;
        private int refs = 0;

        private SharedEmbeddedBroker() throws Exception {
            org.apache.activemq.artemis.core.config.Configuration cfg =
                    new ConfigurationImpl()
                            .setName("notify-broker")
                            .setPersistenceEnabled(false)
                            .setSecurityEnabled(false)
                            .setJMXManagementEnabled(false)
                            .addAcceptorConfiguration("invm", "vm://0");
            broker = new EmbeddedActiveMQ().setConfiguration(cfg);
            broker.start();
        }

        static synchronized SharedEmbeddedBroker acquire() throws Exception {
            if (instance == null) {
                instance = new SharedEmbeddedBroker();
            }
            instance.refs++;
            return instance;
        }

        synchronized void release() throws Exception {
            if (--refs <= 0 && instance != null) {
                broker.stop();
                instance = null;
            }
        }
    }
}
