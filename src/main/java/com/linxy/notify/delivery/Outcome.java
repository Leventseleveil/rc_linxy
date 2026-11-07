package com.linxy.notify.delivery;

import java.util.List;

import org.springframework.stereotype.Component;

import com.linxy.notify.supplier.SupplierSpec;

/**
 * 投递结果分类 —— 对齐 mockingbird PushStatus 的核心思想：
 * 不能只分"成功/失败"，必须区分"失败后该不该重试"，否则要么丢消息、要么重试风暴。
 */
public final class Outcome {

    private Outcome() {}

    public enum Kind { SUCCESS, RETRYABLE, NON_RETRYABLE, TRANSPORT_ERROR }

    public record Result(Kind kind, Integer httpStatus, String note) {

        public static Result success(int status) { return new Result(Kind.SUCCESS, status, null); }

        /** 明确收到响应但不认：5xx / 429 → 可重试 */
        public static Result retryable(int status, String note) { return new Result(Kind.RETRYABLE, status, note); }

        /** 明确收到响应且重试无意义：4xx（鉴权/参数/格式错误）→ 死信 */
        public static Result fatal(int status, String note) { return new Result(Kind.NON_RETRYABLE, status, note); }

        /** 未收到响应（连接失败/超时）：状态未知 → 可重试（at-least-once，对方需幂等） */
        public static Result transport(String note) { return new Result(Kind.TRANSPORT_ERROR, null, note); }
    }

    /** HTTP 状态码 → 投递结果 分类 */
    public static Result classify(int status) {
        if (status >= 200 && status < 300) return Result.success(status);
        if (status == 429) return Result.retryable(status, "rate limited by vendor");
        if (status >= 500) return Result.retryable(status, "vendor server error");
        return Result.fatal(status, "non-retryable client error");
    }
}
