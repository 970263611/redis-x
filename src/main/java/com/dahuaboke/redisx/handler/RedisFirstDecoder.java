package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Constant;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class RedisFirstDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(RedisFirstDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        while(in.isReadable()) {
            in.markReaderIndex();
            switch (in.getByte(in.readerIndex())) {
                case '+':
                case '-':
                case ':':
                    int index = ByteBufUtil.indexOf(Constant.SEPARAPOR,in);
                    if(index != -1){
                        out.add(Unpooled.copiedBuffer(in.readBytes(index - in.readerIndex() + 2)));
                        break;
                    }else{
                        in.resetReaderIndex();
                        return;
                    }
                case '$':
                    ByteBuf dollarbuf = Unpooled.buffer();
                    boolean dollerflag = dollarCut(dollarbuf,in);
                    if(dollerflag){
                        out.add(dollarbuf);
                        break;
                    }else{
                        in.resetReaderIndex();
                        dollarbuf.release();
                        return;
                    }
                case '*':
                    List<ByteBuf> starlist = new ArrayList<>();
                    boolean starflag = starCut(starlist,in);
                    if(starflag){
                        out.add(starlist);
                        break;
                    }else{
                        in.resetReaderIndex();
                        starlist.forEach(b -> {
                            b.release();
                        });
                        return;
                    }
                default:
                    break;
            }
        }
    }

    private boolean starCut(List<ByteBuf> copylist,ByteBuf in){
        int index = ByteBufUtil.indexOf(Constant.SEPARAPOR,in);
        if(index == -1){
            return false;
        }
        ByteBuf starbuf = Unpooled.copiedBuffer(in.readBytes(1));//把*号加进去
        ByteBuf lenbuf = in.readBytes(index - in.readerIndex());
        int len = Integer.parseInt(lenbuf.toString(Charset.defaultCharset()));
        starbuf.writeBytes(lenbuf);//把长度加进去
        starbuf.writeBytes(in,2);//把长度后的\r\n加进去
        copylist.add(starbuf);
        while(len > 0){
            len--;
            ByteBuf copybuf = Unpooled.buffer();
            if(!dollarCut(copybuf,in)){
                return false;
            }
            copylist.add(copybuf);
        }
        return true;
    }

    private boolean dollarCut(ByteBuf copybuf,ByteBuf in){
        int index = ByteBufUtil.indexOf(Constant.SEPARAPOR,in);
        if(index == -1){
            return false;
        }
        copybuf.writeBytes(in.readBytes(1));//把$加进去
        ByteBuf lenbuf = in.readBytes(index - in.readerIndex());
        int len = Integer.parseInt(lenbuf.toString(Charset.defaultCharset()));
        copybuf.writeBytes(lenbuf);//把长度加进去
        copybuf.writeBytes(in,2);//把长度后的\r\n加进去
        if((in.writerIndex() - in.readerIndex()) >= (len + 2)){
            copybuf.writeBytes(in,len + 2);//把内容和内容后的\r\n加进去
            return true;
        }else{
            return false;
        }
    }


}
