package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
        Thread thread = new Thread(() -> {
            Channel channel = ctx.channel();
            int flushThreshold = 0;
            long timeThreshold = System.currentTimeMillis();
            while (!toContext.isClose()) {
                if (channel.pipeline().get(Constant.SLOT_HANDLER_NAME) == null &&
                        channel.isActive() && channel.isWritable()) {
                    CacheManager.CommandReference reference = toContext.listen();
                    boolean immediate = toContext.isImmediate();
                    if (reference != null) {
                        FromContext fromContext = reference.getFromContext();
                        Integer length = reference.getLength();
                        String command = reference.getContent();
                        long offset = fromContext.getOffset();
                        if (length != null) {
                            offset += length;
                            fromContext.setOffset(offset);
                        }
                        if (immediate) { //强一致模式
                            for (int i = 0; i < toContext.getImmediateResendTimes(); i++) {
                                boolean success = immediateSend(ctx, command, length, i + 1);
                                if (success) {
                                    break;
                                }
                            }
                        } else {
                            ctx.write(command);
                            flushThreshold++;
                        }
                        logger.debug("Write command [{}] length [{}], now offset [{}]", command, length, offset);
                    }
                    if (!immediate && (flushThreshold > 100 || (System.currentTimeMillis() - timeThreshold > 100))) {
                        ctx.flush();
                        logger.trace("Flush data success");
                        flushThreshold = 0;
                        timeThreshold = System.currentTimeMillis();
                    }
                }
            }
        });
        thread.setName(Constant.PROJECT_NAME + "-To-Writer-" + toContext.getHost() + ":" + toContext.getPort());
        thread.start();
    }

    private boolean immediateSend(ChannelHandlerContext ctx, String command, int length, int times) {
        if (!ctx.writeAndFlush(command).isSuccess() || toContext.preemptMasterCompulsoryWithCheckId()) {
            logger.error("Write command [{}] length [{}] error times: [" + times + "]", command, length);
            return false;
        }
        return true;
    }
}