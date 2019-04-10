package com.bonc.utils;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by changkaituo on 2018/6/22.
 */
public class Kutil {
    public static String getString(JSONObject jsonObject, String key, String defaultValue) {
        if (jsonObject.containsKey(key)) {
            String value= jsonObject.getString(key);
            if(value!=null){
                return value;
            }else{
                return "null";
            }
        }
        return defaultValue;
    }
}
