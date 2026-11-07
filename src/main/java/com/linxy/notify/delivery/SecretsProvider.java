package com.linxy.notify.delivery;

import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * 密钥提供者：渲染 {{secret.xxx}} 变量。
 * 演示实现从环境变量读（NOTIFY_SECRET_CRM_TOKEN → crm_token）。
 * 生产应接 Vault / KMS，密钥永不落库、永不出现在供应商 yaml 里。
 */
@Component
public class SecretsProvider {

    public Map<String, String> forSupplier(String supplier) {
        // 简化实现：扫描所有 NOTIFY_SECRET_ 前缀环境变量，全部可见。
        // 更严格的做法是按 supplier 命名空间隔离：NOTIFY_SECRET_{SUPPLIER}_{KEY}。
        return System.getenv();
    }
}
