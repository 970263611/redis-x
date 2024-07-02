package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.from.SyncCommand;
import com.dahuaboke.redisx.from.FromContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/9 10:11
 * auth: dahua
 * desc:
 */
public class PostDistributeHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger logger = LoggerFactory.getLogger(PostDistributeHandler.class);
    private FromContext fromContext;

    public PostDistributeHandler(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) throws Exception {
        Channel channel = ctx.channel();
        if (channel.isActive() && channel.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) != null) {
            String reply = ((SimpleStringRedisMessage) msg).content();
            ctx.channel().attr(Constant.SYNC_REPLY).set(reply);
        } else {
            ctx.fireChannelRead(new SyncCommand(fromContext, msg, true));
        }
    }
}