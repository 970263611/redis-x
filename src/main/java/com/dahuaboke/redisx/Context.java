package com.dahuaboke.redisx;

import com.dahuaboke.redisx.handler.SlotInfoHandler;
import com.dahuaboke.redisx.utils.CRC16;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingDeque;

/**
 * 2024/5/13 11:09
 * auth: dahua
 * desc:
 */
public class Context {

    private static final Logger logger = LoggerFactory.getLogger(Context.class);
    protected BlockingDeque<String> replyQueue;
    protected SlotInfoHandler.SlotInfo slotInfo;
    protected boolean isClose = false;
    protected boolean isConsole;
    protected boolean toIsCluster;
    protected boolean fromIsCluster;

    public Context(boolean fromIsCluster, boolean toIsCluster) {
        this.fromIsCluster = fromIsCluster;
        this.toIsCluster = toIsCluster;
    }

    public boolean isAdapt(boolean isCluster, String key) {
        return false;
    }

    public void setSlotInfo(SlotInfoHandler.SlotInfo slotInfo) {
        this.slotInfo = slotInfo;
    }

    protected int calculateHash(String command) {
        String[] ary = command.split(" ");
        if (ary.length > 1) {
            return CRC16.crc16(ary[1].getBytes(StandardCharsets.UTF_8));
        } else {
            return CRC16.crc16(command.getBytes(StandardCharsets.UTF_8));
        }
    }

    public boolean isClose() {
        return isClose;
    }

    public void setClose(boolean close) {
        isClose = close;
    }

    public boolean isToIsCluster() {
        return toIsCluster;
    }

    public boolean isFromIsCluster() {
        return fromIsCluster;
    }

    public boolean isConsole() {
        return isConsole;
    }
}
