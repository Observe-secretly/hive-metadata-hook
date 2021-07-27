package cn.tsign.conf;

public enum ConfProperties {

    HOOK_TOPIC("HOOK_TOPIC","TOPIC-HIVE-EXECUTE-HOOK"),
    bootstrap_servers("bootstrap.servers","kafka01:6667,kafka02:6667"),

    auto_commit_interval_ms("auto.commit.interval.ms",100),
    auto_commit_enable("auto.commit.enable","false"),
    enable_auto_commit("enable.auto.commit","false"),
    session_timeout_ms("session.timeout.ms","30000");


    public String key;
    public Object value;

    private ConfProperties(String key,Object value){
        this.key = key;
        this.value = value;
    }
}


