package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * 2024/6/12 17:10
 * auth: dahua
 * desc: 主备策略处理器
 */
public class DRHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger logger = LoggerFactory.getLogger(DRHandler.class);
    private ToContext toContext;
    //10次标志不变从升为主
    private final LimitedList<String> limitedList = new LimitedList(10) {{
        add("INIT");
    }};

    public DRHandler(Context toContext) {
        this.toContext = (ToContext) toContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String reply) throws Exception {
        if (ctx.pipeline().get(Constant.SLOT_HANDLER_NAME) == null &&
                reply.startsWith(Constant.PROJECT_NAME)) {
            String[] split = reply.split("\\|");
            if (split.length != 4) {
                toContext.preemptMasterCompulsory();
            } else {
                if (!Constant.PROJECT_NAME.equals(split[0])) {
                    toContext.preemptMasterCompulsory();
                } else {
                    if (toContext.getId().equals(split[1])) { //主节点是自己
                        toContext.isMaster(true);
                        toContext.preemptMasterCompulsory();
                    } else { //主节点非自己
                        toContext.isMaster(false);
                        toContext.clearAllNodeMessages();
                        String nodeMessagesStr = split[2];
                        if (!"".equals(nodeMessagesStr)) {
                            String[] nodeMessagesSplit = nodeMessagesStr.split(";");
                            for (String nodeMessageStr : nodeMessagesSplit) {
                                String[] messageSplit = nodeMessageStr.split("&", -1);
                                String[] hostAndPort = messageSplit[0].split(":");
                                String host = hostAndPort[0];
                                int port = Integer.parseInt(hostAndPort[1]);
                                String masterId = messageSplit[1];
                                long offset = Long.parseLong(messageSplit[2]);
                                logger.trace("Sync host [{}] port [{}] masterId [{}] offset [{}]", host, port, masterId, offset);
                                toContext.setNodeMessage(host, port, masterId, offset);
                            }
                        }
                        //这里不用时间区间判断是因为无法保证各服务器时间相同
                        String random = split[3];
                        limitedList.add(random);
                        if (limitedList.checkNeedUpgradeMaster()) {
                            toContext.preemptMasterCompulsory();
                        }
                    }
                }
            }
        } else {
            if (reply.startsWith(Constant.ERROR_REPLY_PREFIX)) {
                logger.error("Receive redis error reply [{}]", reply);
            }
            if (toContext.isConsole()) {
                toContext.callBack(reply);
            }
        }
    }

    private class LimitedList<L> extends LinkedList<L> {
        private final int limitSize;

        public LimitedList(int limitSize) {
            this.limitSize = limitSize;
        }

        @Override
        public boolean add(L l) {
            super.add(l);
            while (size() > limitSize) {
                remove();
            }
            return true;
        }

        public boolean checkNeedUpgradeMaster() {
            L previous = get(0);
            for (int i = 1; i < size(); i++) {
                L v = get(i);
                if (!v.equals(previous)) {
                    return false;
                } else {
                    previous = v;
                }
            }
            return true;
        }
    }
}
