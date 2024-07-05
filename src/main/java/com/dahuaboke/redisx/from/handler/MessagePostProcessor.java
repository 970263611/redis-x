package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.command.from.SyncCommand;
import com.dahuaboke.redisx.command.from.SyncCommand2;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 15:42
 * auth: dahua
 * desc: 指令后置处理器
 */
public class MessagePostProcessor extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(MessagePostProcessor.class);

    public MessagePostProcessor(Context context) {
        super(context);
    }

    @Override
    public void channelRead1(ChannelHandlerContext ctx, SyncCommand syncCommand) throws Exception {
        try {
            if (syncCommand != null) {
                ctx.fireChannelRead(syncCommand);
            }
        } catch (CodecException e) {
            logger.error("Command cannot be parsed: {}", syncCommand, e);
        }
    }

    @Override
    public void channelRead2(ChannelHandlerContext ctx, String reply) throws Exception {
        throw new RuntimeException();
    }
}
