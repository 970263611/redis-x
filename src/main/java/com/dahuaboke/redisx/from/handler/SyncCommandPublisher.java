package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.command.from.SyncCommand;
import com.dahuaboke.redisx.from.FromContext;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/9 17:26
 * auth: dahua
 * desc:
 */
public class SyncCommandPublisher extends SimpleChannelInboundHandler<SyncCommand> {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandPublisher.class);
    private FromContext fromContext;
    private long unSyncCommandLength = 0;

    public SyncCommandPublisher(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SyncCommand command) throws Exception {
        if (command.getKey() == null) {
            unSyncCommandLength += command.getLength();
        } else {
            command.setContext(fromContext);
            command.setSyncLength(unSyncCommandLength + command.getLength());
            unSyncCommandLength = 0;
            boolean success = fromContext.publish(command);
            if (success) {
                logger.debug("Sync success length [{}] command \r\n [{}]" ,command.getLength() ,ByteBufUtil.prettyHexDump(command.getByteBuf()));
            } else {
                logger.error("Sync failed length [{}] command \r\n [{}]" ,command.getLength() ,ByteBufUtil.prettyHexDump(command.getByteBuf()));
            }
        }
    }
}
