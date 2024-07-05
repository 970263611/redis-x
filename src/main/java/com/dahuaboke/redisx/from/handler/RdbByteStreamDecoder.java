package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.from.RdbCommand;
import com.dahuaboke.redisx.command.from.SyncCommand;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.from.rdb.CommandParser;
import com.dahuaboke.redisx.from.rdb.RdbData;
import com.dahuaboke.redisx.from.rdb.RdbInfo;
import com.dahuaboke.redisx.from.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: Rdb文件解析处理类
 */
public class RdbByteStreamDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);

    private RdbType rdbType = RdbType.START;
    private int length = -1;
    private ByteBuf eofEnd = null;
    private ByteBuf tempRdb = null;
    private CommandParser commandParser = new CommandParser();
    private FromContext fromContext;

    private enum RdbType {
        START,
        TYPE_EOF,
        TYPE_LENGTH,
        END;
    }

    public RdbByteStreamDecoder(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RdbCommand) {
            ByteBuf rdb = ((RdbCommand) msg).getIn();

            if (RdbType.START == rdbType && '$' == rdb.getByte(rdb.readerIndex())) {
                rdb.readByte();//除去$
                int index = ByteBufUtil.indexOf(Constant.SEPARAPOR, rdb);//找到首个\r\n的index
                String isEofOrSizeStr = rdb.readBytes(index - rdb.readerIndex()).toString(CharsetUtil.UTF_8);//截取\r\n之前的内容，不包含\r\n
                logger.info("RdbType is " + rdbType.name() + ",isEofOrSizeStr " + isEofOrSizeStr);
                rdb.readBytes(Constant.SEPARAPOR.readableBytes());//除去首个\r\n
                tempRdb = ByteBufAllocator.DEFAULT.buffer();//创建缓存
                if (isEofOrSizeStr.startsWith("EOF")) {//EOF类型看收尾
                    eofEnd = Unpooled.copiedBuffer(isEofOrSizeStr.substring(4, isEofOrSizeStr.length()).getBytes());//获取eof收尾的40位长度的校验码
                    rdbType = RdbType.TYPE_EOF;//设置解析类型位eof类型
                    logger.info("RdbType is " + rdbType.name() + ",Rdb EOF is " + eofEnd.toString(Charset.defaultCharset()) + ",current data length is = " + tempRdb.readableBytes());
                } else {//LENGTH类型数长度
                    length = Integer.parseInt(isEofOrSizeStr);//获取rdb总长度
                    rdbType = RdbType.TYPE_LENGTH;//设置解析类型位length类型
                    logger.info("RdbType is " + rdbType.name() + ",RdbLength is " + length + ",current data length is = " + tempRdb.readableBytes());
                }
            }

            ByteBuf rdbBuf = null;//需要解析的rdb内容
            ByteBuf commondBuf = null;//如果发生粘包，这里存放除了Rdb的后续内容

            if (rdb.readableBytes() != 0) {
                if (RdbType.TYPE_LENGTH == rdbType) {
                    tempRdb.writeBytes(rdb);//把内容写入缓存
                    if (length == tempRdb.readableBytes()) {//如果rdb总长正好等于缓存大小，则开始执行解析，并且不存在需要向后发布的内容
                        rdbBuf = tempRdb;
                        rdbType = RdbType.END;
                    } else if (length < tempRdb.readableBytes()) {//如果rdb总长小于缓存大小，则拆分缓存，开始执行解析，并且存在需要向后发布的内容
                        rdbBuf = tempRdb.slice(0, length);
                        commondBuf = tempRdb.slice(length, tempRdb.writerIndex() - length);
                        rdbType = RdbType.END;
                    }
                    logger.info("RdbType is " + rdbType.name() + ",RdbLength is " + length + ",current data length is = " + tempRdb.readableBytes());
                } else if (RdbType.TYPE_EOF == rdbType) {
                    int searchIndex = tempRdb.writerIndex() >= 40 ? tempRdb.writerIndex() - 40 : 0;//防止拆包，从总体缓存的后40位开始检索
                    tempRdb.writeBytes(rdb);//把内容写入缓存
                    tempRdb.readerIndex(searchIndex);//防止拆包，从总体缓存的后40位开始检索
                    int index = ByteBufUtil.indexOf(eofEnd, tempRdb);//检索
                    if (index != -1) {
                        rdbType = RdbType.END;
                        tempRdb.readerIndex(0);//读标识设置为0
                        rdbBuf = tempRdb.slice(0, index + 40);//rdb全部内容 index + 40位的eof
                        commondBuf = tempRdb.slice(index + 40, tempRdb.writerIndex() - index - 40);//命令内容
                    }
                    tempRdb.readerIndex(0);
                    logger.info("RdbType is " + rdbType.name() + ",current data length is = " + tempRdb.readableBytes());
                }

                if (RdbType.END == rdbType) {//开始解析Rdb
                    length = -1;
                    eofEnd = null;
                    rdbType = RdbType.START;
                    ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                    if (fromContext.isSyncRdb()) {
                        fromContext.setRdbAckOffset(true);
                        ByteBuf finalRdbBuf = rdbBuf;
                        CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                            String threadName = Constant.PROJECT_NAME + "-RdbParseThread-" + fromContext.getHost() + ":" + fromContext.getPort();
                            Thread.currentThread().setName(threadName);
                            parse(finalRdbBuf);
                            return null;
                        });
                        future.exceptionally(e -> {
                            logger.error("Parse rdb stream error", e);
                            return null;
                        });
                        while (!future.isDone()) {
                            if (!fromContext.isClose()) {
                                fromContext.ackOffset();
                            }
                            Thread.sleep(1000);
                        }
                        fromContext.setRdbAckOffset(false);
                        logger.info("The RDB stream has been processed");
                    }
                }
                if (commondBuf != null && commondBuf.isReadable()) {//命令如何有内容，继续往下走
                    ctx.fireChannelRead(commondBuf);
                } else {
                    if (rdbBuf != null) {
                        rdbBuf.release();
                    }
                }
            }
            rdb.release();
        } else {
            ctx.fireChannelRead(msg);
        }

    }

    private void parse(ByteBuf byteBuf) {
        RdbParser parser = new RdbParser(byteBuf);
        RdbInfo info = parser.getRdbInfo();
        while (true) {
            parser.parse();
            if(info.isEnd()){
                break;
            }
            //TODO 待处理
//            if(info.isDataReady()){
//                RdbData data = info.getRdbData();
//                if (data.getDataNum() == 1) {
//                    long selectDB = data.getSelectDB();
//                    SyncCommand syncCommand2 = new SyncCommand(fromContext, new ArrayList<String>() {{
//                        add(Constant.SELECT);
//                        add(String.valueOf(selectDB));
//                    }}, false);
//                    boolean success2 = fromContext.publish(syncCommand2);
//                    if (success2) {
//                        logger.debug("Select db success [{}]", selectDB);
//                    } else {
//                        logger.error("Select db failed [{}]", selectDB);
//                    }
//                }
//                List<List<String>> commands = commandParser.parser(data);
//                for (List<String> command : commands) {
//                    SyncCommand syncCommand1 = new SyncCommand(fromContext, command, false);
//                    boolean success1 = fromContext.publish(syncCommand1);
//                    if (success1) {
//                        logger.trace("Success rdb data [{}]", commands);
//                    } else {
//                        logger.error("Sync rdb data [{}] failed", commands);
//                    }
//                }
//            }
        }
    }
}
