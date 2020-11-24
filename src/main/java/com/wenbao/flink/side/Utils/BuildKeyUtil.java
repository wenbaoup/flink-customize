package com.wenbao.flink.side.Utils;

import java.util.List;
import java.util.Map;

public class BuildKeyUtil {

    private BuildKeyUtil() {
    }

    public static String buildKey(List<Object> equalValList) {
        StringBuilder sb = new StringBuilder("");
        for (Object equalVal : equalValList) {
            sb.append(equalVal).append("_");
        }

        return sb.toString();
    }


    public static String buildKey(Map<String, Object> val, List<String> equalFieldList) {
        StringBuilder sb = new StringBuilder();
        for (String equalField : equalFieldList) {
            sb.append(val.get(equalField)).append("_");
        }
        return sb.toString();
    }
}
