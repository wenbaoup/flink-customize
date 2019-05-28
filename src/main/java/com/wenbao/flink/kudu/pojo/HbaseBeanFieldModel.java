package com.wenbao.flink.kudu.pojo;

import java.lang.reflect.Method;

public class HbaseBeanFieldModel {
    private Method getMethod;
    private Method setMethod;
    private String fieldName;
    private Class<?> fieldType;

    public HbaseBeanFieldModel() {
    }

    public Method getGetMethod() {
        return this.getMethod;
    }

    public void setGetMethod(Method getMethod) {
        this.getMethod = getMethod;
    }

    public Method getSetMethod() {
        return this.setMethod;
    }

    public void setSetMethod(Method setMethod) {
        this.setMethod = setMethod;
    }

    public Class<?> getFieldType() {
        return this.fieldType;
    }

    public void setFieldType(Class<?> fieldType) {
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }


}
