package com.wenbao.flink.mysql.source;

import com.google.common.collect.ImmutableMap;
import com.wenbao.flink.mysql.source.cdc.CanalJsonEvent;
import com.wenbao.flink.mysql.source.cdc.MysqlCDCConstants;
import com.wenbao.flink.mysql.source.core.RecordCursorInternal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static com.wenbao.flink.mysql.source.cdc.MysqlCDCConstants.*;

public class TiDBSchemaAdapter implements Serializable {
    
    private final DataType physicalDataType;
    private final int physicalFieldCount;
    private final String[] physicalFieldNames;
    private final DataType[] physicalFieldTypes;
    private final int producedFieldCount;
    private final TypeInformation<RowData> typeInfo;
    private final Map<String, String> properties;
    
    public TiDBSchemaAdapter(ResolvedCatalogTable table,
            Function<DataType, TypeInformation<RowData>> typeInfoFactory, int[] projectedFields) {
        ResolvedSchema schema = table.getResolvedSchema();
        
        Field[] physicalFields = schema.getColumns()
                .stream().filter(Column::isPhysical).map(c ->
                        DataTypes.FIELD(c.getName(),
                                DataTypeUtils.removeTimeAttribute(c.getDataType()))
                ).toArray(Field[]::new);
        
        if (projectedFields != null) {
            Field[] projectedPhysicalFields = new Field[projectedFields.length];
            for (int idx = 0; idx < projectedFields.length; ++idx) {
                projectedPhysicalFields[idx] = physicalFields[projectedFields[idx]];
            }
            physicalFields = projectedPhysicalFields;
        }
        
        this.physicalDataType = DataTypes.ROW(physicalFields).notNull();
        this.physicalFieldNames = Arrays.stream(physicalFields)
                .map(Field::getName).toArray(String[]::new);
        this.physicalFieldTypes = Arrays.stream(physicalFields)
                .map(Field::getDataType).toArray(DataType[]::new);
        this.physicalFieldCount = physicalFieldNames.length;
        
        final DataType producedDataType;
        producedDataType = physicalDataType;
        producedFieldCount = physicalFieldCount;
        this.typeInfo = typeInfoFactory.apply(producedDataType);
        this.properties = table.getOptions();
    }
    
    
    private Object[] makeRow() {
        int metaIndex = physicalFieldCount;
        Object[] objects = new Object[producedFieldCount];
        return objects;
    }
    
    public String[] getPhysicalFieldNames() {
        return physicalFieldNames;
    }
    
    public GenericRowData convert(RecordCursorInternal cursor) {
        Object[] objects = makeRow();
        for (int idx = 0; idx < physicalFieldCount; idx++) {
            
            objects[idx] = toRowDataType(
                    getObjectWithDataType(cursor.getObject(idx), physicalFieldTypes[idx]));
        }
        return GenericRowData.ofKind(RowKind.INSERT, objects);
    }
    
    
    public GenericRowData kafkaDataConvert(Map<String, Object> value, String type,
            CanalJsonEvent canalJsonEvent) {
        Object[] objects = makeRow();
        for (int idx = 0; idx < physicalFieldCount; idx++) {
            String physicalFieldName = physicalFieldNames[idx];
            if (physicalFieldName.startsWith(MATE_TIDB_PREFIX)) {
                toMetaData(canalJsonEvent, objects, idx, physicalFieldName);
                continue;
            }
            objects[idx] = toRowDataType(
                    getObjectWithDataType(value.get(physicalFieldName),
                            physicalFieldTypes[idx]));
        }
        switch (type) {
            case MysqlCDCConstants.INSERT:
                return GenericRowData.ofKind(RowKind.INSERT, objects);
            case MysqlCDCConstants.UPDATE_BEFORE:
                return GenericRowData.ofKind(RowKind.UPDATE_BEFORE, objects);
            case MysqlCDCConstants.UPDATE_AFTER:
                return GenericRowData.ofKind(RowKind.UPDATE_AFTER, objects);
            case MysqlCDCConstants.DELETE:
                return GenericRowData.ofKind(RowKind.DELETE, objects);
            default:
                return null;
        }
    }
    
    private void toMetaData(CanalJsonEvent canalJsonEvent, Object[] objects, int idx,
            String physicalFieldName) {
        switch (physicalFieldName) {
            case MATE_TIDB_TYPE:
                objects[idx] = toRowDataType(
                        getObjectWithDataType(canalJsonEvent.getType(),
                                physicalFieldTypes[idx]));
                break;
            case MATE_TIDB_TS:
                objects[idx] = toRowDataType(
                        getObjectWithDataType(canalJsonEvent.getTs(),
                                physicalFieldTypes[idx]));
                break;
            case MATE_TIDB_ES:
                objects[idx] = toRowDataType(
                        getObjectWithDataType(canalJsonEvent.getEs(),
                                physicalFieldTypes[idx]));
                break;
        }
    }
    
    // These two methods were copied from flink-base as some interfaces changed in 1.13 made
    // it very hard to reuse code in flink-base
    private static Object stringToFlink(Object object) {
        return StringData.fromString(object.toString());
    }
    
    private static Object bigDecimalToFlink(Object object) {
        BigDecimal bigDecimal = (BigDecimal) object;
        return DecimalData.fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
    }
    
    private static Object localDateToFlink(Object object) {
        LocalDate localDate = (LocalDate) object;
        return (int) localDate.toEpochDay();
    }
    
    private static Object localDateTimeToFlink(Object object) {
        return TimestampData.fromLocalDateTime((LocalDateTime) object);
    }
    
    private static Object localTimeToFlink(Object object) {
        LocalTime localTime = (LocalTime) object;
        return (int) (localTime.toNanoOfDay() / (1000 * 1000));
    }
    
    public static Map<Class<?>, Function<Object, Object>> ROW_DATA_CONVERTERS =
            ImmutableMap.of(
                    String.class, TiDBSchemaAdapter::stringToFlink,
                    BigDecimal.class, TiDBSchemaAdapter::bigDecimalToFlink,
                    LocalDate.class, TiDBSchemaAdapter::localDateToFlink,
                    LocalDateTime.class, TiDBSchemaAdapter::localDateTimeToFlink,
                    LocalTime.class, TiDBSchemaAdapter::localTimeToFlink
            );
    
    /**
     * transform Row type to RowData type
     */
    public static Object toRowDataType(Object object) {
        if (object == null) {
            return null;
        }
        
        Class<?> clazz = object.getClass();
        if (!ROW_DATA_CONVERTERS.containsKey(clazz)) {
            return object;
        } else {
            return ROW_DATA_CONVERTERS.get(clazz).apply(object);
        }
    }
    
    /**
     * transform TiKV java object to Flink java object by given Flink Datatype
     *
     * @param object    TiKV java object
     * @param flinkType Flink datatype
     *                  //     * @param tidbType  TiDB datatype
     */
    public static Object getObjectWithDataType(@Nullable Object object, DataType flinkType) {
        if (object == null) {
            return null;
        }
        Class<?> conversionClass = flinkType.getConversionClass();
        if (flinkType.getConversionClass() == object.getClass()) {
            return object;
        }
        switch (conversionClass.getSimpleName()) {
            case "String":
                if (object instanceof byte[]) {
                    object = new String((byte[]) object);
                } else if (object instanceof Timestamp) {
                    Timestamp timestamp = (Timestamp) object;
//                    object = timestamp.toLocalDateTime().format(formatter);
                } else if (object instanceof Long) {
                    // covert tidb timestamp to flink string
//                    object = new Timestamp(((long) object) / 1000).toLocalDateTime()
//                            .format(formatter);
                } else {
                    object = object.toString();
                }
                break;
            case "Integer":
//                object = (int) (long)
//                        getObjectWithDataType(object, DataTypes.BIGINT(), tidbType, formatter)
//                                .orElseThrow(() -> new IllegalArgumentException(
//                                        "Failed to convert integer"));
                if (object instanceof String) {
                    object = Integer.valueOf(object.toString());
                }
                if (object instanceof Boolean) {
                    object = (Boolean) object ? 1 : 0;
                }
                break;
            case "Long":
                if (object instanceof LocalDate) {
                    object = ((LocalDate) object).toEpochDay();
                } else if (object instanceof LocalDateTime) {
                    object = Timestamp.valueOf(((LocalDateTime) object)).getTime();
                } else if (object instanceof LocalTime) {
                    object = ((LocalTime) object).toNanoOfDay();
                } else if (object instanceof String) {
                    object = Long.valueOf(object.toString());
                } else if (object instanceof BigInteger) {
                    object = Long.valueOf(object.toString());
                }
                break;
            case "LocalDate":
                if (object instanceof Date) {
                    object = ((Date) object).toLocalDate();
                } else if (object instanceof String) {
                    object = LocalDate.parse((String) object);
                } else if (object instanceof Long || object instanceof Integer) {
                    object = LocalDate.ofEpochDay(Long.parseLong(object.toString()));
                }
                break;
            case "LocalDateTime":
                if (object instanceof Timestamp) {
                    object = ((Timestamp) object).toLocalDateTime();
                } else if (object instanceof String) {
                    String tmp = (String) object;
                    if (19 == tmp.length()) {
                        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        object = LocalDateTime.parse((String) object, df);
                    } else if (21 == tmp.length()) {
                        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
                        object = LocalDateTime.parse((String) object, df);
                    }
                } else if (object instanceof Long) {
                    object = new Timestamp(((Long) object) / 1000).toLocalDateTime();
                }
                break;
            case "LocalTime":
                if (object instanceof Long || object instanceof Integer) {
                    object = LocalTime.ofNanoOfDay(Long.parseLong(object.toString()));
                }
                break;
            case "BigDecimal":
                if (object instanceof Double || object instanceof Float ||
                        object instanceof String) {
                    object = new BigDecimal(object.toString());
                }
                break;
            default:
        }
        return object;
    }
    
    public DataType getPhysicalDataType() {
        return physicalDataType;
    }
    
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }
    
}
