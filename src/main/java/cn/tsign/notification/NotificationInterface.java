package cn.tsign.notification;

import java.util.List;

import cn.tsign.exceptions.NotificationException;

public interface NotificationInterface {

    void send(String... messages) throws NotificationException;


    void send(List<String> messages) throws NotificationException;


    void close();
}