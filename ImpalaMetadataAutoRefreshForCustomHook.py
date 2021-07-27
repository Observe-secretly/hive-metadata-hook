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

# 获取impala连接
def get_conn(host, port):
    conn = connect(host=host, port=port)
    return conn


# 执行Impala SQL
def impala_query(conn, sql):
    if is_debug:
        print('\033[36m QUERY: \033[0m')
        print('\033[36m' + sql + '\033[0m')
        print(' ')
    cur = conn.cursor()
    cur.execute(sql)
    data_list = cur.fetchall()
    return data_list


def impala_exec(conn, sql):
    if is_debug:
        print('\033[31m EXEC: \033[0m')
        print('\033[31m' + sql + '\033[0m')
        print(' ')
    cur = conn.cursor()
    cur.execute(sql)

def handel(hookContent):
    try:

        #获取impala连接
        conn = conn = get_conn(impalaHost, impalaPort)
        #print hookContent
        #转换成json对象
        jsonObj = json.loads(hookContent)
        #获取需要刷新的表
        tables = jsonObj['tables']
        ##循环获取待刷新表,逐个刷新（一般只会有一个表）
        for table in tables:
            impala_exec(conn, 'invalidate metadata ' + table + ';')
            impala_exec(conn, 'select * from ' + table + ' limit 1;')
            print jsonObj['operationName']+'-->invalidate metadata ' + table + ';'

        conn.close()
    except Exception as e:
        print e

if __name__ == '__main__':
    # 消费者
    topic = client.topics[topicName]
    consumer = topic.get_simple_consumer(consumer_group='impalaMetadataAutoRefresh', auto_commit_enable=True,auto_commit_interval_ms=3000, consumer_id='impalaMetadataAutoRefresh')
    for message in consumer:
        if message is not None:
            #得到自定义Hook监听在hive元数据变动的内容。分析得到操作类型和表名称。使用impala刷新元数据
            handel(message.value)

