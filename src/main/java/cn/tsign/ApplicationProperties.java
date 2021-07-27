package cn.tsign;

import cn.tsign.conf.ConfProperties;
import cn.tsign.exceptions.HiveHookException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Application properties.
 */
public final class ApplicationProperties {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);

    private static volatile Configuration instance = null;

    public static Configuration get() throws HiveHookException {
        if (instance == null) {
            synchronized (ApplicationProperties.class) {
                if (instance == null) {
                    instance = load();
                }
            }
        }
        return instance;
    }

    public static Configuration load() throws HiveHookException {
        BaseConfiguration configuration = new BaseConfiguration();
        for (ConfProperties item: ConfProperties.values()) {
            configuration.addProperty(item.key,item.value);
        }

        logConfiguration(configuration);
        return configuration;
    }

    private static void logConfiguration(Configuration configuration) {
        if (LOG.isDebugEnabled()) {
            Iterator<String> keys = configuration.getKeys();
            LOG.debug("Configuration loaded:");
            while (keys.hasNext()) {
                String key = keys.next();
                LOG.debug("{} = {}", key, configuration.getProperty(key));
            }
        }
    }

}