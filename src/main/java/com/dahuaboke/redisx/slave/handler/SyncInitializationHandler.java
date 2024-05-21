package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.slave.SlaveContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dahuaboke.redisx.slave.handler.SyncInitializationHandler.State.*;

/**
 * 2024/5/8 11:13
 * auth: dahua
 * desc: 发起同步处理器
 */
public class SyncInitializationHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(SyncInitializationHandler.class);

    private SlaveContext slaveContext;

    public SyncInitializationHandler(SlaveContext slaveContext) {
        this.slaveContext = slaveContext;
    }

    enum State {
        INIT,
        SENT_PING,
        SENT_PORT,
        SENT_ADDRESS,
        SENT_CAPA,
        SENT_PSYNC;

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        if (channel.isActive()) {
            Thread thread = new Thread(() -> {
                State state = null;
                String reply;
                while (!slaveContext.isClose()) {
                    if (channel.pipeline().get(Constant.SLOT_HANDLER_NAME) == null) {
                        if ((reply = channel.attr(Constant.SYNC_REPLY).get()) != null) {
                            if (state == INIT) {
                                state = SENT_PING;
                            }
                            if (Constant.PONG_COMMAND.equalsIgnoreCase(reply) && state == SENT_PING) {
                                clearReply(ctx);
                                state = SENT_PORT;
                                channel.writeAndFlush(Constant.CONFIG_PORT_COMMAND_PREFIX + slaveContext.getLocalPort());
                                logger.debug("Sent replconf listening-port command");
                            }
                            if (Constant.OK_COMMAND.equalsIgnoreCase(reply) && state == SENT_PORT) {
                                clearReply(ctx);
                                state = SENT_ADDRESS;
                                channel.writeAndFlush(Constant.CONFIG_HOST_COMMAND_PREFIX + slaveContext.getLocalHost());
                                logger.debug("Sent replconf address command");
                                continue;
                            }
                            if (Constant.OK_COMMAND.equalsIgnoreCase(reply) && state == SENT_ADDRESS) {
                                clearReply(ctx);
                                state = SENT_CAPA;
                                channel.writeAndFlush(Constant.CONFIG_CAPA_COMMAND);
                                logger.debug("Sent replconf capa eof command");
                                continue;
                            }
                            if (Constant.OK_COMMAND.equalsIgnoreCase(reply) && state == SENT_CAPA) {
                                clearReply(ctx);
                                state = SENT_PSYNC;
                                channel.writeAndFlush(Constant.CONFIG_ALL_PSYNC_COMMAND);
                                logger.debug("Sent psync ? -1 command");
                            }
                            if (state == SENT_PSYNC) {
                                ChannelPipeline pipeline = channel.pipeline();
                                pipeline.remove(this);
                                logger.debug("Sent all sync command");
                                break;
                            }
                        } else {
                            if (state == null) {
                                state = INIT;
                                channel.writeAndFlush(Constant.PING_COMMAND);
                                logger.debug("Sent ping command");
                            }
                        }
                    }
                }
            });
            thread.setName(Constant.PROJECT_NAME + "-SYNC-INIT-" + slaveContext.getMasterHost() + ":" + slaveContext.getMasterPort());
            thread.start();
        }
    }

    private void clearReply(ChannelHandlerContext ctx) {
        ctx.channel().attr(Constant.SYNC_REPLY).set(null);
    }
}