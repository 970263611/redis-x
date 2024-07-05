package com.dahuaboke.redisx.from.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintBufHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(PrintBufHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof ByteBuf){
            ByteBuf byteBuf = (ByteBuf)msg;
            StringBuilder sb = new StringBuilder();
            sb.append("\r\n<").append(Thread.currentThread().getName()).append(">").append("<redis massage> = ").append(byteBuf).append("\r\n");
            sb.append(ByteBufUtil.prettyHexDump(byteBuf).toString());
            logger.trace(sb.toString());
        }
        super.channelRead(ctx, msg);
    }
}
