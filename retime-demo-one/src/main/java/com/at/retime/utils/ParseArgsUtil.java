package com.at.retime.utils;


import com.at.retime.common.Constant;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @create 2021-10-09
 */
public class ParseArgsUtil {

    private static Logger logger =  LoggerFactory.getLogger(ParseArgsUtil.class);
    private static String path = Constant.DEFAULT_CONFIG_FILE;
    public static final String envType = Constant.ENV_TYPE;

    public static ParameterTool init(String[] args, boolean isSQLSubmit) {

        Map<String, String> inputMap = init(args);

        if(Optional.ofNullable(isSQLSubmit).orElse(false)){
            // if input parameter like: sql=xxx
            if(!inputMap.containsKey(Constant.INPUT_SQL_FILE_PARA)){
                System.out.println("please input sql file. like : --sql sql/demo.sql");
                System.exit(-1);
            }
        }

        if(!inputMap.containsKey(Constant.RUN_TYPE)) inputMap.put(Constant.RUN_TYPE,envType);

        if(inputMap.containsKey(Constant.CONFIG_FILE)){
            path = inputMap.get(Constant.CONFIG_FILE);
        }

        path = new ParseArgsUtil().getClass().getClassLoader().getResource(path).getPath();

        if(!new File(path).exists()){
            throw new RuntimeException("You must specify a profile on resources catalogue or through parameter --conf input");
        }

        // load default properties
        // load default properties : sqlSubmit.properties
        ParameterTool defaultPropFile = null;
        try {
            defaultPropFile = ParameterTool.fromPropertiesFile(path);
        } catch (IOException e) {
            logger.info("Unable to resolve the configuration file in the " + path + " directory");
            e.printStackTrace();
        }
        // load input job properties
        //    var inputJobPropFile: ParameterTool = null
        if(inputMap.containsKey(Constant.INPUT_JOB_PROP_FILE_PARA)){
            inputMap.put(Constant.INPUT_JOB_PROP_FILE_PARA, defaultPropFile.get(Constant.INPUT_JOB_PROP_FILE_PARA));
        }

        ParameterTool parameterTool = defaultPropFile.mergeWith(ParameterTool.fromMap(inputMap));

        return parameterTool;

    }


    private static Map<String, String> init(String[] args) {

        Map<String, String> map = new HashMap(args.length / 2);
        int i = 0;


        while (i < args.length) {
            String key;

            // 必须以 "--" 或 "-" 开头
            if (args[i].startsWith("--")) {
                key = args[i].substring(2);
            } else {
                if (!args[i].startsWith("-")) {
                    throw new IllegalArgumentException(String.format("Error parsing arguments '%s' on '%s'. Please prefix keys with -- or -.", Arrays.toString(args), args[i]));
                }

                key = args[i].substring(1);
            }

            if (key.isEmpty()) {
                throw new IllegalArgumentException("The input " + Arrays.toString(args) + " contains an empty argument");
            }

            // k=v 形式
            if (key.contains("=")) {
                int splitIndex = key.indexOf("=");
                if (splitIndex == key.length() - 1) {
                    throw new IllegalArgumentException("The input " + key + " is illegal.Its value is empty.It must be \"--k=v\" or \"--k=\"v1 v2\"\".");
                }
                String newKey = key.substring(0, splitIndex);
                String value = key.substring(splitIndex + 1);
                map.put(newKey, value);
                i += 1;
                continue;
            }

            ++i;
            if (i >= args.length) {
                map.put(key, "__NO_VALUE_KEY");
            } else if (NumberUtils.isNumber(args[i])) {
                map.put(key, args[i]);
                ++i;
            } else if (!args[i].startsWith("--") && !args[i].startsWith("-")) {
                map.put(key, args[i]);
                ++i;
            } else {
                map.put(key, "__NO_VALUE_KEY");
            }
        }

        return map;


    }


    public static void main(String[] args) throws IOException {

        ParameterTool parameterTool = ParseArgsUtil.init(args, false);
        System.out.println(parameterTool.toMap());

    }

}
