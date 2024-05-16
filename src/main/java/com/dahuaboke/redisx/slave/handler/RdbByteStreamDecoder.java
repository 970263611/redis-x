package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.RdbCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: Rdb文件解析处理类
 */
public class RdbByteStreamDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);
    private int rdbSize = 0;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RdbCommand) {
            RdbCommand rdb = (RdbCommand) msg;
            logger.info("Now processing the RDB stream");
            ByteBuf in = rdb.getIn();
            try {
                //设置标记索引，防止多次暂存，后面利用浅拷贝
                int index = 0;
                while (in.isReadable()) {
                    byte b = in.readByte();
                    if (b == '\n') {
                        break;
                    }
                    if (b != '\r') {
                        index++;
                    }
                }
                if (index > 0) {
                    if ('$' == in.getByte(0)) {
                        ByteBuf tempBuf = in.slice(1, index - 1);
                        String rdbSizeCommand = tempBuf.toString(CharsetUtil.UTF_8);
                        rdbSize = Integer.parseInt(rdbSizeCommand);
                        System.out.println("rdbsize-111111111111111111111111111111111111111-" + rdbSize);
                        if (in.readableBytes() == rdbSize) {
                            //index + 2 跳过\r\n
                            ByteBuf rdbStream = in.slice(index + 2, rdbSize);
                            // TODO
                            logger.info("The RDB stream has been processed");
                            ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                        }
                        //else 流是分开的，需要等下一次处理
                    } else if ('R' == in.getByte(0)) {
                        ByteBuf rdbStream = in.slice(0, rdbSize);
                        // TODO
                        System.out.println("分开流111111111111111111111111111111111111111-" + rdbSize);
                        logger.info("The RDB stream has been processed");
                        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                    } else {
                        //无法识别流，为了避免特殊数据，所以传播到下游
                        logger.error("Unknown RDB stream format");
                        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                        ctx.fireChannelRead(in);
                    }
                }
                //else 空指令跳过
            } finally {
                in.release();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
