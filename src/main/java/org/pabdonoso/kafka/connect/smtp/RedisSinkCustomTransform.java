package org.pabdonoso.kafka.connect.smtp;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSinkCustomTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOG = LoggerFactory.getLogger("RedisCustomTransform");

    public static final String REGEX_KEY_CONFIG = "regexKey";
    public static final String REGEX_VALUE_CONFIG = "regexValue";
    public static final String REGEX_KEY_DOC = "Property that defines the regex pattern to apply for the message Key";
    public static final String REGEX_VALUE_DOC = "Property that defines the regex pattern to apply for the message Value";
    public static final String keySeparator = ":";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(REGEX_KEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,  ConfigDef.Importance.HIGH,
                    REGEX_KEY_DOC)
            .define(REGEX_VALUE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    REGEX_VALUE_DOC);

    private String regexKey;
    private String regexValue;

    private Pattern patternKey;
    private Pattern pattenrValue;


    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        regexKey = config.getString(REGEX_KEY_CONFIG);
        regexValue = config.getString(REGEX_VALUE_CONFIG);

        pattenrValue = Pattern.compile(regexValue);
        patternKey = Pattern.compile(regexKey);
    }




    @Override
    public void close() {
    }

    @Override
    public R apply(R record) {
        LOG.info("==> Key: "+record.key().toString()+".");
        LOG.info("==> Value: "+record.value().toString()+".");
        String key = record.topic() + ":" + getData(record.key().toString(), patternKey);
        String value = getData(record.value().toString(), pattenrValue);
        LOG.info("==> Parse Key: "+key+".");
        LOG.info("==> Parse Value: "+value+".");
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), key, record.valueSchema(),
                value, record.timestamp());
    }


    public static String getData(final String data, final Pattern pattern ){

        final Matcher matcherKey = pattern.matcher(data);
        String dataPattern = "";
        while (matcherKey.find()) {
            dataPattern+=matcherKey.group(1)+keySeparator;
        }
        if(dataPattern.endsWith(keySeparator)){
            dataPattern = dataPattern.substring(0,dataPattern.length()-1);
        }
        return  dataPattern;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}
