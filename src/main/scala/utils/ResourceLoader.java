package utils;

/**
 * Created by Michael on 2016/12/1.
 */
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class ResourceLoader {

    private static ResourceLoader loader = new ResourceLoader();
    private static Map<String, Properties> loaderMap = new HashMap<String, Properties>();

    private ResourceLoader() {
    }

    public static ResourceLoader getInstance() {
        return loader;
    }

    public Properties getPropFromProperties(String fileName) throws Exception {

        Properties prop = loaderMap.get(fileName);
        if (prop != null) {
            return prop;
        }
        String filePath = null;
        String configPath = System.getProperty("configurePath");

        if (configPath == null) {
            filePath = this.getClass().getClassLoader().getResource(fileName).getPath();
        } else {
            filePath = configPath + "/" + fileName;
        }
        prop = new Properties();
        prop.load(new FileInputStream(new File(filePath)));

        loaderMap.put(fileName, prop);
        return prop;
    }

    public  void test(){
        String filePath = this.getClass().getClassLoader().getResource("data.properties").getPath();
        System.out.println(filePath);
    }
    public static void main(String[] args) {
        //String tmp = PropertiesUtils.getStringByKey("gd_py_corp_sharehd_info", "data.properties");
        ResourceLoader r = new ResourceLoader();
        r.test();
    }

}
