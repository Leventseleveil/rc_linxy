package com.linxy.notify.api;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 入向鉴权：Bearer token 白名单（演示级）。
 * token 配置格式 "token1,token2"；生产建议 JWT/内部签名服务 + 按 source 隔离配额。
 */
@Component
public class TokenAuth {

    private final List<String> tokens;

    public TokenAuth(@Value("${notify.security.api-tokens:dev-token-1}") String tokensCfg) {
        this.tokens = java.util.Arrays.stream(tokensCfg.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    public boolean check(String authorizationHeader) {
        if (authorizationHeader == null) return false;
        String token = authorizationHeader.startsWith("Bearer ")
                ? authorizationHeader.substring(7).trim()
                : authorizationHeader.trim();
        return tokens.contains(token);
    }

    /** 演示：当前 token 对应的来源系统标识（生产应从 token 反查） */
    public String currentSource() {
        return "biz-system";
    }
}
