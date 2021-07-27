package cn.tsign.notification;

import cn.tsign.exceptions.HiveHookException;
import cn.tsign.exceptions.NotificationException;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration.Configuration;

import java.util.Arrays;
import java.util.List;

/**
 * Abstract notification interface implementation.
 */
public abstract class AbstractNotification implements NotificationInterface {

    //// each char can encode upto 4 bytes in UTF-8
    public static final int MAX_BYTES_PER_CHAR = 4;

    public AbstractNotification(Configuration applicationProperties) throws HiveHookException {
    }

    @VisibleForTesting
    protected AbstractNotification() {
    }

    @Override
    public void send(List<String> messages) throws NotificationException {
        sendInternal(messages);
    }

    @Override
    public void send(String... messages) throws NotificationException {
        send(Arrays.asList(messages));
    }

    protected abstract void sendInternal(List<String> messages) throws NotificationException;
}