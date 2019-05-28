package com.wenbao.flink.kudu.utils;

import java.lang.reflect.Field;

public class FieldUtility {
    private static final String getPrefix = "get";
    private static final String isPrefix = "is";
    private static final String setPrefix = "set";

    public FieldUtility() {
    }

    public static String getGetMethodName(Field field) {
        return getGetMethodName(field.getName(), field.getType());
    }

    public static String getSetMethodName(Field field) {
        return getSetMethodName(field.getName(), field.getType());
    }

    public static String getSetMethodName(String fieldName, Class<?> fieldType) {
        if (fieldType.equals(Boolean.class) && fieldName.startsWith("is")) {
            fieldName = fieldName.substring(2);
        }

        StringBuilder stringBuilder = new StringBuilder("set");
        stringBuilder.append(fieldName.substring(0, 1).toUpperCase()).append(fieldName.substring(1));
        return stringBuilder.toString();
    }

    public static String getGetMethodName(String fieldName, Class<?> fieldType) {
        if (fieldType.equals(Boolean.class) && fieldName.startsWith("is")) {
            fieldName = fieldName.substring(2);
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(fieldName.substring(0, 1).toUpperCase()).append(fieldName.substring(1));
        return fieldType.equals(Boolean.TYPE) ? stringBuilder.insert(0, "is").toString() : stringBuilder.insert(0, "get").toString();
    }
}
