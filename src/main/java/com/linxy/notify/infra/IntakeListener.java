package com.linxy.notify.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 多业务 intake 队列消费（对应参考系统：peregrine 消费各业务 MQ）。
 *
 * 每个业务系统一个独立队列 notify.intake.<source>（配置 notify.intake-queues），
 * 队列名即来源归属 —— 不信任消息体里的 source 字段，天然防伪造。
 * 队列间相互隔离：某业务消息堆积/毒丸不影响其他业务接入。
 */
@Component
public class IntakeListener {

    private static final Logger log = LoggerFactory.getLogger(IntakeListener.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SubmitService submitter;

    public IntakeListener(SubmitService submitter) {
        this.submitter = submitter;
    }

    @JmsListener(destination = "notify.intake.ads")
    public void onAds(String body) { consume("ads", body); }

    @JmsListener(destination = "notify.intake.crm")
    public void onCrm(String body) { consume("crm", body); }

    @JmsListener(destination = "notify.intake.inventory")
    public void onInventory(String body) { consume("inventory", body); }

    private void consume(String source, String body) {
        try {
            SubmitService.IntakeMessage msg = MAPPER.readValue(body, SubmitService.IntakeMessage.class);
            SubmitService.SubmitOutcome out = submitter.accept(source, msg);
            if (out.accepted()) {
                log.info("[intake:{}] accepted id={}", source, out.notification().getId());
            } else if (out.duplicated()) {
                log.info("[intake:{}] duplicated id={}", source, out.notification().getId());
            } else {
                log.warn("[intake:{}] rejected: {}", source, out.error());
            }
        } catch (Exception e) {
            // 解析失败 = 毒丸消息：记日志不重投（重投也不会好），由审计/告警跟进
            log.error("[intake:{}] bad message: {}", source, body, e);
        }
    }
}
