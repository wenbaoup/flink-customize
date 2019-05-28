package com.wenbao.flink.kudu.utils;

import com.wenbao.flink.kudu.pojo.HbaseBeanFieldModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KuduUtils.class);

    private KuduUtils() {

    }


    public static Map<String, HbaseBeanFieldModel[]> beanFieldModelMap = new HashMap<>();

    private static HbaseBeanFieldModel[] getBeanModel(Class<?> beanClass) throws Exception {
        Field[] fields = beanClass.getDeclaredFields();
        List<HbaseBeanFieldModel> hbaseBeanFieldModelList = new ArrayList<>(fields.length);
        for (Field field : fields) {
            HbaseBeanFieldModel model = new HbaseBeanFieldModel();
            if ("serialVersionUID".equalsIgnoreCase(field.getName())) {
                continue;
            }
            model.setGetMethod(beanClass.getDeclaredMethod(FieldUtility.getGetMethodName(field)));
            model.setSetMethod(beanClass.getDeclaredMethod(FieldUtility.getSetMethodName(field), field.getType()));
            model.setFieldName(field.getName());
            model.setFieldType(field.getType());
            hbaseBeanFieldModelList.add(model);
        }
        return hbaseBeanFieldModelList.toArray(new HbaseBeanFieldModel[0]);
    }


    public static HbaseBeanFieldModel[] getBeanFieldModels(Class<?> beanClass) {
        if (null != beanFieldModelMap.get(beanClass.getSimpleName())) {
            return beanFieldModelMap.get(beanClass.getSimpleName());
        }
        try {
            HbaseBeanFieldModel[] hbaseBeanFieldModels = getBeanModel(beanClass);
            beanFieldModelMap.put(beanClass.getSimpleName(), hbaseBeanFieldModels);
            return hbaseBeanFieldModels;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("反射类失败,类名为:{},具体异常为:{}", beanClass.getSimpleName(), e.getMessage());
        }
        return null;
    }
}
