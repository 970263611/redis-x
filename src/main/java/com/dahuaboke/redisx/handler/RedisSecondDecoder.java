package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.from.SyncCommand2;
import com.dahuaboke.redisx.utils.CRC16;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.charset.Charset;
import java.util.List;

public class RedisSecondDecoder extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof List){
            List<ByteBuf> buflist = (List<ByteBuf>) msg;
            int keypos = -1;
            SyncCommand2 command = new SyncCommand2();
            ByteBuf[] arr = new ByteBuf[buflist.size()];
            for(int i=0; i < buflist.size(); i++){
                arr[i] =  buflist.get(i);
                if(i == 1){
                    findIndex(arr[i]);
                    keypos = Constant.specialCommandPrefix.contains(arr[i].toString(Charset.defaultCharset())) ? 3 : 2;
                    resetIndex(arr[i]);
                }
                if(keypos == i){
                    findIndex(arr[i]);
                    command.setSlot(CRC16.crc16(arr[i]));
                    resetIndex(arr[i]);
                }
            }
            command.setByteBuf(Unpooled.wrappedBuffer(arr));
            command.setLength(command.getByteBuf().readableBytes());
            ctx.fireChannelRead(command);
        }else if(msg instanceof ByteBuf){
            ByteBuf in = (ByteBuf)msg;
            switch (in.readByte()) {
                case '+':
                case '-':
                case ':':
                    int index = ByteBufUtil.indexOf(Constant.SEPARAPOR,in);
                    ctx.fireChannelRead(in.readBytes(index - in.readerIndex()).toString(Charset.defaultCharset()));
                    in.release();
                    break;
                case '$':
                    index = ByteBufUtil.indexOf(Constant.SEPARAPOR,in);
                    int len = Integer.parseInt(in.readBytes(index - in.readerIndex()).toString(Charset.defaultCharset()));
                    in.readBytes(2);
                    ctx.fireChannelRead(in.readBytes(len).toString(Charset.defaultCharset()));
                    in.release();
                    break;
                default:
                    in.release();
                    break;
            }
        }else{
            ctx.fireChannelRead(msg);
        }
    }

    private void findIndex(ByteBuf buf){
        buf.markReaderIndex();
        buf.markWriterIndex();
        buf.readerIndex(ByteBufUtil.indexOf(Constant.SEPARAPOR,buf) + 2);
        buf.writerIndex(buf.writerIndex() - 2);
    }

    private void resetIndex(ByteBuf buf){
        buf.resetWriterIndex();
        buf.resetReaderIndex();
    }

}
