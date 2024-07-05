package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Constant;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/9 10:11
 * auth: dahua
 * desc:
 */
public class PostDistributeHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger logger = LoggerFactory.getLogger(PostDistributeHandler.class);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        Channel channel = ctx.channel();
        if (channel.isActive() && channel.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) != null) {
            ctx.channel().attr(Constant.SYNC_REPLY).set(msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}