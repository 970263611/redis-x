package com.dahuaboke.redisx.from;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.command.from.SyncCommand;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/6 16:18
 * auth: dahua
 * desc: 从节点上下文
 */
public class FromContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(FromContext.class);
    private CacheManager cacheManager;
    private String host;
    private int port;
    private Channel fromChannel;
    private int slotBegin;
    private int slotEnd;
    private FromClient fromClient;
    private boolean isConsole;
    private boolean rdbAckOffset = false;
    private boolean alwaysFullSync;
    private boolean syncRdb;
    private int unSyncCommandLength = 0;
    private String masterId;

    public FromContext(CacheManager cacheManager, String host, int port, boolean isConsole, boolean fromIsCluster, boolean toIsCluster, boolean alwaysFullSync, boolean syncRdb) {
        super(fromIsCluster, toIsCluster);
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.isConsole = isConsole;
        if (isConsole) {
            replyQueue = new LinkedBlockingDeque();
        }
        this.alwaysFullSync = alwaysFullSync;
        this.syncRdb = syncRdb;
    }

    public String getId() {
        return cacheManager.getId();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean publish(SyncCommand command) {
        if (!isConsole) {
            return cacheManager.publish(command);
        } else {
            if (replyQueue == null) {
                throw new IllegalStateException("By console mode replyQueue need init");
            } else {
                //TODO 待处理
                //return replyQueue.offer(command.getStringCommand());
                return true;
            }
        }
    }

    public void setFromChannel(Channel fromChannel) {
        this.fromChannel = fromChannel;
    }

    public String getLocalHost() {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) this.fromChannel.localAddress();
        return inetSocketAddress.getHostString();
    }

    public int getLocalPort() {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) this.fromChannel.localAddress();
        return inetSocketAddress.getPort();
    }

    public void setFromClient(FromClient fromClient) {
        this.fromClient = fromClient;
    }

    @Override
    public boolean isAdapt(boolean isMasterCluster, String command) {
        if (isMasterCluster && command != null) {
            int hash = calculateHash(command) % Constant.COUNT_SLOT_NUMS;
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ToContext类型的context
            return true;
        }
    }

    public boolean isConsole() {
        return isConsole;
    }

    public void setSlotBegin(int slotBegin) {
        this.slotBegin = slotBegin;
    }

    public void setSlotEnd(int slotEnd) {
        this.slotEnd = slotEnd;
    }

    @Override
    public String sendCommand(Object command, int timeout) {
        if (replyQueue == null) {
            throw new IllegalStateException("By console mode replyQueue need init");
        } else {
            replyQueue.clear();
            fromClient.sendCommand((String) command);
            try {
                return replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return null;
            }
        }
    }

    public void close() {
        this.fromClient.destroy();
    }

    public long getOffset() {
        return cacheManager.getNodeMessage(host, port).getOffset();
    }

    public void setOffset(long offset) {
        cacheManager.setNodeMessage(this.host, this.port, masterId, offset);
    }

    public CacheManager.NodeMessage getNodeMessage() {
        return cacheManager.getNodeMessage(this.host, this.port);
    }

    public void ackOffset() {
        if (fromChannel != null && fromChannel.isActive() && fromChannel.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) == null) {
            Long offsetSession = fromChannel.attr(Constant.OFFSET).get();
            CacheManager.NodeMessage nodeMessage = getNodeMessage();
            if (offsetSession != null && offsetSession > -1L) {
                if (nodeMessage == null) {
                    setOffset(offsetSession);
                }
                fromChannel.attr(Constant.OFFSET).set(-1L);
            }
            if (nodeMessage != null) {
                long offset = getOffset() + unSyncCommandLength;
                fromChannel.writeAndFlush(Constant.ACK_COMMAND_PREFIX + offset);
                logger.trace("Ack offset [{}]", offset);
            }
        }
    }

    public boolean isRdbAckOffset() {
        return rdbAckOffset;
    }

    public void setRdbAckOffset(boolean rdbAckOffset) {
        this.rdbAckOffset = rdbAckOffset;
    }

    public String getPassword() {
        return cacheManager.getFromPassword();
    }

    public boolean isAlwaysFullSync() {
        return alwaysFullSync;
    }

    public boolean redisVersionBeyond3() {
        return cacheManager.getRedisVersion().charAt(0) > '3';
    }

    public boolean isSyncRdb() {
        return syncRdb;
    }

    public int getUnSyncCommandLength() {
        return unSyncCommandLength;
    }

    public void appendUnSyncCommandLength(int length) {
        this.unSyncCommandLength += length;
    }

    public void clearUnSyncCommandLength() {
        this.unSyncCommandLength = 0;
    }

    public void setMasterId(String masterId) {
        this.masterId = masterId;
    }
}
