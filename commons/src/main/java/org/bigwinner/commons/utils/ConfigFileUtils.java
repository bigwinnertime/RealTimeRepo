package org.bigwinner.commons.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @author: bigwinner
 * @date: 2021/6/20 下午12:22
 * @version: 1.0.0
 * @description: 配置文件加载工具类
 */
public class ConfigFileUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigFileUtils.class);
    static Properties properties;

    public ConfigFileUtils(String configFileName) {
        properties = new Properties();
        try {
            properties.load(ConfigFileUtils.class.getClassLoader().getResourceAsStream(configFileName));
        } catch (IOException e) {
            LOGGER.error("配置文件加载失败：{}", e.getCause());
        }
    }

    public String getValueFromProperties(String key) {
        String property = properties.getProperty(key);
        return property.trim();
    }
}
