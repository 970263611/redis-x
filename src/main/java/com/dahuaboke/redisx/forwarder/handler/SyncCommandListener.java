package com.dahuaboke.redisx.forwarder.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.forwarder.ForwarderContext;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:37
 * auth: dahua
 * desc:
 */
public class SyncCommandListener extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandListener.class);
    private ForwarderContext forwarderContext;

    public SyncCommandListener(ForwarderContext forwarderContext) {
        this.forwarderContext = forwarderContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Thread thread = new Thread(() -> {
            while (!forwarderContext.isClose()) {
                if (ctx.channel().pipeline().get(Constant.SLOT_HANDLER_NAME) == null) {
                    String command = forwarderContext.listen();
                    if (command != null) {
                        ctx.writeAndFlush(command);
                        logger.debug("Write command success [{}]", command);
                    }
                }
            }
        });
        thread.setName(Constant.PROJECT_NAME + "-Forwarder-Writer-" + forwarderContext.getForwardHost() + ":" + forwarderContext.getForwardPort());
        thread.start();
    }

    @Override
    public void channelRead1(ChannelHandlerContext ctx, String reply) throws Exception {
        logger.debug("Receive redis reply [{}]", reply);
        forwarderContext.callBack(reply);
    }
}
