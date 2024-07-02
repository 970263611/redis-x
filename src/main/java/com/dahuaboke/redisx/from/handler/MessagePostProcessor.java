package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 15:42
 * auth: dahua
 * desc: 指令后置处理器
 */
public class MessagePostProcessor extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(MessagePostProcessor.class);
    private FromContext fromContext;

    public MessagePostProcessor(FromContext fromContext) {
        super(fromContext);
        this.fromContext = fromContext;
    }

    @Override
    public void channelRead2(ChannelHandlerContext ctx, String reply) throws Exception {
        fromContext.publishForConsole(reply);
    }
}
