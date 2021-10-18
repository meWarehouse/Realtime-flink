package com.at.loggercreate.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.springframework.boot.system.ApplicationHome;

/**
 * @author zero
 * @create 2021-06-26 17:41
 */
public class ConfigUtil {
    public ConfigUtil() {
    }

    public static String loadJsonFile(String fileName) {
        String filePath = getJarDir() + "/" + fileName;
        File file = new File(filePath);
        Object resourceAsStream = null;

        try {
            if (file.exists()) {
                resourceAsStream = new FileInputStream(file);
            } else {
                resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
            }

            String json = IOUtils.toString((InputStream)resourceAsStream, "utf-8");
            return json;
        } catch (IOException var5) {
            var5.printStackTrace();
            throw new RuntimeException("配置文件" + fileName + "读取异常");
        }
    }

    public static String getJarDir() {
        File file = getJarFile();
        return file == null ? null : file.getParent();
    }

    private static File getJarFile() {
        ApplicationHome h = new ApplicationHome(ConfigUtil.class);
        File jarF = h.getSource();
        System.out.println(jarF.getParentFile().toString());
        return jarF;
    }
}
