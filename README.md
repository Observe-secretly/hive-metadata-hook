# 背景
之前上传过一段借助Atlas Hook的实现Impala元数据刷新的Python脚本([传送门](https://github.com/Observe-secretly/AutoRefreshImpala)).最近发现它无法监听到某些Hive操作，比如增加分区操作。那么这段代码将演示如何自定义一个Hive Hook把监听的数据发送到Kafka，并通过编写的Python脚本刷新Impala元数据

# 功能
- 订阅Hive元数据变动
- 刷新Impala元数据

# 如何使用

## 关于配置

### *Hook配置*
``` java
public enum ConfProperties {
    HOOK_TOPIC("HOOK_TOPIC","TOPIC-HIVE-EXECUTE-HOOK"),
    bootstrap_servers("bootstrap.servers","kafka01:6667,kafka02:6667"),
......
}
```
Hook中请手动修改`ConfProperties.java`配置文件，请正确配置`bootstrap_servers`项（kafka BrokerServer地址。例：kafka01:6667,kafka02:6667）。

### *脚本配置*
```python
#!/usr/bin/python
# coding=utf8

##运行方式 nohup python -u impalaMetadataAutoRefreshForCustomHook.py > run.log &

import json
from impala.dbapi import connect
from pykafka import KafkaClient
import sys
reload(sys)
sys.setdefaultencoding('utf8')

is_debug = False

#kafka config
client = KafkaClient(hosts="kafka01:6667,kafka02:6667")
topicName = 'TOPIC-HIVE-EXECUTE-HOOK'

#impala config
impalaHost = '{impala_host}'
impalaPort = 21050
......
```
和Hook中的`bootstrap_servers`一样，请正确配置kafka config栏目中的kafka hosts配置。另外还需要配置impala config的`impalaHost`配置项。

## Hook部署
- 1、在项目根目录下（pom文件同级）执行`mvn clean package`进行编译
- 2、找到HiveServer所在的机器和安装目录（下面以Ambari的目录为例）
- 3、将打包好的`hive-metadata-hook.jar`上传到HiveServer安装目录的lib文件夹内。参考命令（pom文件同级下执行）:
``` shell
scp target/hive-metadata-hook.jar root@host:/usr/hdp/{HDP-VERSION}/hive/lib
```
- 4、修改Hive配置.在配置项`hive.exec.post.hooks`中追加`cn.tsign.MetadataOpMonitorHook`。多个Hook使用英文逗号隔开
- 5、重启Hive。问题跟踪与定位，可观察HiveServer上的`/var/log/hive/hiveserver2.log`（默认位置）日志

## 脚本部署
```
nohup python -u impalaMetadataAutoRefreshForCustomHook.py > run.log &
```
*温馨提示*：impala依赖请参考如下命令`pip install  six bit_array thrift==0.9.3 thrift_sasl==0.2.1 pure_sasl  impyla==0.10.0`

## Hook扩展
本示例只监听了作者需要监听的事件，实际上Hive元数据变动的事件非常丰富，有多丰富呢？121种吧。若需要扩展只需要修改`MetadataOpMonitorHook.java`的静态代码块：
``` java 
......

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
        OPERATION_NAMES.add(HiveOperation.CREATEDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPDATABASE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.DROPTABLE.getOperationName());
        OPERATION_NAMES.add(HiveOperation.LOAD.getOperationName());
        OPERATION_NAMES.add(HiveOperation.REPLLOAD.getOperationName());
    }

......
}
```

# 问题交流反馈
QQ：914245697
