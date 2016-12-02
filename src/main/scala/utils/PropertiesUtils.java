package utils;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
/**
 * Created by Michael on 2016/12/1.
 */

public  class PropertiesUtils {
    private static ResourceLoader loader = ResourceLoader.getInstance();
    private static ConcurrentMap<String, String> configMap = new ConcurrentHashMap<String, String>();
    private static final String DEFAULT_CONFIG_FILE = "data.properties";

    private static Properties prop = null;

    public static String getStringByKey(String key, String propName) {
        try {
            prop = loader.getPropFromProperties(propName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        key = key.trim();
        if (!configMap.containsKey(key)) {
            if (prop.getProperty(key) != null) {
                configMap.put(key, prop.getProperty(key));
            }
        }
        return configMap.get(key);
    }

    public static String getStringByKey(String key) {
        return getStringByKey(key, DEFAULT_CONFIG_FILE);
    }

    public static Properties getProperties() {
        try {
            return loader.getPropFromProperties(DEFAULT_CONFIG_FILE);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        String tmp = PropertiesUtils.getStringByKey("gd_py_corp_sharehd_info");
        System.out.println(tmp);
    }
}