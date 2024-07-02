package com.dahuaboke.redisx.command.from;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.Command;
import com.dahuaboke.redisx.from.FromContext;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;

import java.util.List;

/**
 * 2024/5/9 9:37
 * auth: dahua
 * desc:
 */
public class SyncCommand extends Command {

    private FromContext fromContext;
    private RedisMessage redisMessage;
    private int length;
    private boolean needAddLengthToOffset;

    public SyncCommand(FromContext fromContext, RedisMessage redisMessage, boolean needAddLengthToOffset) {
        this.fromContext = fromContext;
        this.redisMessage = redisMessage;
        this.needAddLengthToOffset = needAddLengthToOffset;
    }

    public RedisMessage getRedisMessage() {
        return redisMessage;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public FromContext getFromContext() {
        return fromContext;
    }

    public boolean isNeedAddLengthToOffset() {
        return needAddLengthToOffset;
    }

    public int getMessageLength() {
        try {
            ArrayRedisMessage arrayRedisMessage = (ArrayRedisMessage) redisMessage;
            return getArrayMessageLength(arrayRedisMessage);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public boolean isIgnoreMessage() {
        String content = getStringByIndexFromMessage(0);
        if (content.toUpperCase().startsWith(Constant.SELECT)) {
            return fromContext.isFromIsCluster() || fromContext.isToIsCluster();
        }
        return Constant.PING_COMMAND.equalsIgnoreCase(content);
    }

    public String getKey() {
        return getStringByIndexFromMessage(1);
    }

    private String getStringByIndexFromMessage(int index) {
        RedisMessage msg = ((ArrayRedisMessage) redisMessage).children().get(index);
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        } else if (msg instanceof FullBulkStringRedisMessage) {
            return ((FullBulkStringRedisMessage) msg).content().toString(CharsetUtil.UTF_8);
        }
        throw new IllegalArgumentException();
    }

    private int getArrayMessageLength(ArrayRedisMessage msg) {
        List<RedisMessage> children = msg.children();
        int size = children.size();
        int length = 1 + String.valueOf(size).length() + 2 + size * 5;
        for (RedisMessage child : msg.children()) {
            if (child instanceof FullBulkStringRedisMessage) {
                long stringLength = ((FullBulkStringRedisMessage) child).content().readableBytes();
                length += (int) stringLength + String.valueOf(stringLength).length();
            } else if (child instanceof ArrayRedisMessage) {
                length += getArrayMessageLength(((ArrayRedisMessage) child));
            }
        }
        return length;
    }
}
