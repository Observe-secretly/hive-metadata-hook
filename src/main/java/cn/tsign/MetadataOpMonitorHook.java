package cn.tsign;

import cn.tsign.entity.NotifyMessage;
import cn.tsign.notification.NotificationInterface;
import cn.tsign.notification.NotificationProvider;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.hooks.*;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author limin
 */
public class MetadataOpMonitorHook implements ExecuteWithHookContext {

    protected static NotificationInterface notificationInterface;

    private static Logger logger = LoggerFactory.getLogger(MetadataOpMonitorHook.class);
    private static final HashSet<String> OPERATION_NAMES = new HashSet<>();



    static {
        notificationInterface = NotificationProvider.get();

        OPERATION_NAMES.add(HiveOperation.CREATETABLE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE_OWNER.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_ADDCOLS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_LOCATION.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_PROPERTIES.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_RENAME.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_RENAMECOL.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_REPLACECOLS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERTABLE_ADDPARTS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERDATABASE_LOCATION.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATEDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPTABLE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.LOAD.getOperationName());
        OPERATION_NAMES.add(HiveOperation.REPLLOAD.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
        OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERVIEW_RENAME.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPVIEW.getOperationName());

    }

    @Override
    public void run(HookContext hookContext) throws Exception {
        assert (hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK);

        QueryPlan plan = hookContext.getQueryPlan();

        String operationName = plan.getOperationName();
        logWithHeader("Operation: " + operationName);
        logWithHeader("Query executed: " + plan.getQueryString());

        if (OPERATION_NAMES.contains(operationName)
                    && !plan.isExplain()) {

            NotifyMessage message  = new NotifyMessage();
            message.setExecutedQuery(plan.getQueryString());
            message.setOperationName(operationName);

            Set<ReadEntity> inputs = hookContext.getInputs();
            Set<WriteEntity> outputs = hookContext.getOutputs();

            for (Entity entity : inputs) {
                message.addInput(toJson(entity));
                if(entity.getTable()!=null){
                    message.addTables(entity.getTable().getDbName(),entity.getTable().getTableName());
                }
            }

            for (Entity entity : outputs) {
                message.addOutput(toJson(entity));
                if(entity.getTable()!=null){
                    message.addTables(entity.getTable().getDbName(),entity.getTable().getTableName());
                }

            }

            notificationInterface.send(JSON.toJSONString(message));

        } else {
            logWithHeader("Non-monitored Operation, ignoring hook");
        }
    }

    private static String toJson(Entity entity) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        switch (entity.getType()) {
            case DATABASE:
                Database db = entity.getDatabase();
                return mapper.writeValueAsString(db);
            case TABLE:
                return mapper.writeValueAsString(entity.getTable().getTTable());
        }
        return null;
    }

    private void logWithHeader(Object obj){
        logger.info("[CustomHook][Thread: "+Thread.currentThread().getName()+"] | " + obj);
    }

}
