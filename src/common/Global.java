package common;

import storage.Persistence;
import storage.example.Example;
import storage.example.FactoryImpl;
import structure.Factory;

/**
 * 全局的配置对象，单例调用，懒汉式加载
 */
public class Global {

    /**
     * 工厂
     */
    private Factory factory = new FactoryImpl();
    /**
     * 持久化引擎
     */
    private Persistence persistence = new Example();

    /**
     * 唯一全局对象
     */
    private static Global global = new Global();

    /**
     * 私有构造方法
     */
    private Global() {}

    public static Global getInstance() {
        return global;
    }

    public Factory getFactory() {
        return factory;
    }

    public Persistence getPersistence() {
        return persistence;
    }
}
