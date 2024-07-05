package com.dahuaboke.redisx.to;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.command.from.SyncCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/13 10:38
 * auth: dahua
 * desc:
 */
public class ToContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(ToContext.class);
    private static final String lua1 = "local v = redis.call('GET',KEYS[1]);\n" + "    if v then\n" + "        return v;\n" + "    else\n" + "        local result = redis.call('SET',KEYS[1],ARGV[1]);\n" + "        return result;\n" + "    end";
    private static final String lua2 = "local v = redis.call('GET',KEYS[1]);\n" + "if string.match(v,ARGV[1]) then\n redis.call('SET',KEYS[1],ARGV[2]);\nend";
    private CacheManager cacheManager;
    private String host;
    private int port;
    private int slotBegin;
    private int slotEnd;
    private ToClient toClient;
    private boolean immediate;
    private int immediateResendTimes;
    private String switchFlag;
    private int flushSize;

    public ToContext(CacheManager cacheManager, String host, int port, boolean fromIsCluster, boolean toIsCluster, boolean isConsole, boolean immediate, int immediateResendTimes, String switchFlag, int flushSize) {
        super(fromIsCluster, toIsCluster);
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.isConsole = isConsole;
        if (isConsole) {
            replyQueue = new LinkedBlockingDeque();
        }
        this.immediate = immediate;
        this.immediateResendTimes = immediateResendTimes;
        this.switchFlag = switchFlag;
        this.flushSize = flushSize;
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

    public SyncCommand listen() {
        return cacheManager.listen(this);
    }

    public boolean callBack(String reply) {
        if (isConsole) {
            if (replyQueue == null) {
                throw new IllegalStateException("By console mode replyQueue need init");
            } else {
                //针对控制台返回特殊处理
                if (reply == null) {
                    reply = "(null)";
                }
                return replyQueue.offer(reply);
            }
        } else {
            return true;
        }
    }

    @Override
    public boolean isAdapt(boolean toIsCluster, String command) {
        if (toIsCluster && command != null) {
            int hash = calculateHash(command) % Constant.COUNT_SLOT_NUMS;
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ToContext类型的context
            return true;
        }
    }

    @Override
    public String sendCommand(Object command, int timeout) {
        return sendCommand(command, timeout, false, null);
    }

    public String sendCommand(Object command, int timeout, String key) {
        return sendCommand(command, timeout, false, key);
    }

    public String sendCommand(Object command, int timeout, boolean unCheck) {
        return sendCommand(command, timeout, unCheck, null);
    }

    public String sendCommand(Object command, int timeout, boolean unCheck, String key) {
        if (isConsole) {
            if (replyQueue == null) {
                throw new IllegalStateException("By console mode replyQueue need init");
            } else {
                replyQueue.clear();
                toClient.sendCommand(command);
                try {
                    return replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    return null;
                }
            }
        } else {
            if (unCheck) {
                return String.valueOf(toClient.sendCommand(command, true));
            } else {
                List<Context> allContexts = cacheManager.getAllContexts();
                for (Context context : allContexts) {
                    if (context instanceof ToContext && command instanceof String) {
                        ToContext toContext = (ToContext) context;
                        String flag;
                        if (key != null) {
                            flag = key;
                        } else {
                            flag = (String) command;
                        }
                        if (toContext.isAdapt(toIsCluster, flag)) {
                            return toContext.sendCommand(command, 1000, true, null);
                        }
                    }
                }
                throw new IllegalArgumentException(String.valueOf(command));
            }
        }
    }

    public void setSlotBegin(int slotBegin) {
        this.slotBegin = slotBegin;
    }

    public void setSlotEnd(int slotEnd) {
        this.slotEnd = slotEnd;
    }

    public void setClient(ToClient toClient) {
        this.toClient = toClient;
    }

    public void close() {
        this.toClient.destroy();
    }

    public void isMaster(boolean isMaster) {
        cacheManager.setIsMaster(isMaster);
    }

    public Map<String, CacheManager.NodeMessage> getAllNodeMessages() {
        return cacheManager.getAllNodeMessages();
    }

    public void setNodeMessage(String host, int port, String masterId, long offset) {
        cacheManager.setNodeMessage(host, port, masterId, offset);
    }

    public void clearAllNodeMessages() {
        cacheManager.clearAllNodeMessages();
    }

    public void preemptMaster() {
        List<String> commands = new ArrayList() {{
            add("EVAL");
            add(lua1);
            add("1");
            add(switchFlag);
            add(preemptMasterCommand());
        }};
        this.sendCommand(commands, 1000, true);
    }

    public void preemptMasterCompulsory() {
        this.sendCommand(buildPreemptMasterCompulsoryCommand(), 1000, switchFlag);
    }

    public boolean preemptMasterCompulsoryWithCheckId() {
        List<String> commands = new ArrayList() {{
            add("EVAL");
            add(lua2);
            add("1");
            add(switchFlag);
            add(getId());
            add(preemptMasterCommand());
        }};
        return Boolean.valueOf(this.sendCommand(commands, 1000, switchFlag));
    }

    public String buildPreemptMasterCompulsoryCommand() {
        return "set " + switchFlag + " " + preemptMasterCommand();
    }

    private String preemptMasterCommand() {
        StringBuilder sb = new StringBuilder();
        sb.append(Constant.PROJECT_NAME);
        sb.append("|");
        sb.append(this.getId());
        sb.append("|");
        getAllNodeMessages().forEach((k, v) -> {
            String masterId = v.getMasterId();
            if (masterId != null) {
                sb.append(k);
                sb.append("&");
                sb.append(v.getMasterId());
                sb.append("&");
                sb.append(v.getOffset());
                sb.append(";");
            }
        });
        sb.append("|");
        sb.append(System.currentTimeMillis());
        return new String(sb);
    }

    public String getPassword() {
        return cacheManager.getToPassword();
    }

    public boolean isImmediate() {
        return immediate;
    }

    public int getImmediateResendTimes() {
        return immediateResendTimes;
    }

    public int getFlushSize() {
        return flushSize;
    }
}
