package com.dahuaboke.redisx.command.from;


import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.command.Command;

import io.netty.buffer.ByteBuf;


/**
 * 2024/5/9 9:37
 * auth: dahua
 * desc:
 */
public class SyncCommand extends Command {

    private long length;

    private long syncLength;

    private String key;

    ByteBuf byteBuf;

    private Context context;

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getSyncLength() {
        return syncLength;
    }

    public void setSyncLength(long syncLength) {
        this.syncLength = syncLength;
    }

    public ByteBuf getByteBuf() {
        return byteBuf;
    }

    public void setByteBuf(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

}
