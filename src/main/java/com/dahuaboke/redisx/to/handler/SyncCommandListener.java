package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.command.from.SyncCommand;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:37
 * auth: dahua
 * desc:
 */
public class SyncCommandListener extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandListener.class);
    private ToContext toContext;

    public SyncCommandListener(Context toContext) {
        this.toContext = (ToContext) toContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        int flushSize = toContext.getFlushSize();
        Thread thread = new Thread(() -> {
            Channel channel = ctx.channel();
            int flushThreshold = 0;
            long timeThreshold = System.currentTimeMillis();
            while (!toContext.isClose()) {
                if (channel.pipeline().get(Constant.SLOT_HANDLER_NAME) == null && channel.isActive()) {
                    SyncCommand syncCommand = toContext.listen();
                    boolean immediate = toContext.isImmediate();
                    if (syncCommand != null) {
                        FromContext fromContext = (FromContext) syncCommand.getContext();
                        long offset = fromContext.getOffset();
                        long synclength = syncCommand.getSyncLength();
                        fromContext.setOffset(offset + synclength);
                        if (immediate) { //强一致模式
                            //TODO 待处理
//                            for (int i = 0; i < toContext.getImmediateResendTimes(); i++) {
//                                boolean success = immediateSend(ctx, redisMessage, length, i + 1);
//                                if (success) {
//                                    break;
//                                }
//                            }
                        } else {
                            ctx.write(syncCommand.getByteBuf());
                            flushThreshold++;
                        }
                        logger.trace("Write length [{}] now offset [{}] command \r\n [{}]" ,syncCommand.getLength(), offset, ByteBufUtil.prettyHexDump(syncCommand.getByteBuf()));
                    }
                    if (!immediate && (flushThreshold > flushSize || (System.currentTimeMillis() - timeThreshold > 100))) {
                        if (flushThreshold > 0) {
                            ctx.flush();
                            logger.debug("Flush data success [{}]", flushThreshold);
                            flushThreshold = 0;
                            timeThreshold = System.currentTimeMillis();
                        }
                    }
                }
            }
        });
        thread.setName(Constant.PROJECT_NAME + "-To-Writer-" + toContext.getHost() + ":" + toContext.getPort());
        thread.start();
    }

    private boolean immediateSend(ChannelHandlerContext ctx, RedisMessage redisMessage, int length, int times) {
        if (!ctx.writeAndFlush(redisMessage).isSuccess() || toContext.preemptMasterCompulsoryWithCheckId()) {
            logger.error("Write length [{}] error times: [" + times + "]", length);
            return false;
        }
        return true;
    }
}