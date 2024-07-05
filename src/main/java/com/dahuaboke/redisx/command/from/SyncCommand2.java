package com.dahuaboke.redisx.command.from;


import com.dahuaboke.redisx.command.Command;

import io.netty.buffer.ByteBuf;


/**
 * 2024/5/9 9:37
 * auth: dahua
 * desc:
 */
public class SyncCommand2 extends Command {

    private long length;

    private long syncLength;

    private int slot = -1;

    ByteBuf byteBuf;

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

    public int getSlot() {
        return slot;
    }

    public void setSlot(int slot) {
        this.slot = slot;
    }

    public ByteBuf getByteBuf() {
        return byteBuf;
    }

    public void setByteBuf(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }
}
