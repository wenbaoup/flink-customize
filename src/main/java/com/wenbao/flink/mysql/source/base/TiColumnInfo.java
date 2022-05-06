package com.wenbao.flink.mysql.source.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * TiColumnInfo
 *
 * @author wenbao
 * @since 2022/03/10
 */
public class TiColumnInfo implements Serializable {
    
    private String name;
    private int offset;
    private String type;
    private boolean isPrimaryKey;
    
    
    public TiColumnInfo(String name, int offset, String type, boolean isPrimaryKey) {
        this.name = (Objects.requireNonNull(name, "column name is null")).toLowerCase();
        this.offset = offset;
        this.type = Objects.requireNonNull(type, "data type is null");
        this.isPrimaryKey = isPrimaryKey;
    }
    
    public TiColumnInfo() {
    }
    
    //    static TiColumnInfo getRowIdColumn(int position) {
//        return new TiColumnInfo(-1L, "_tidb_rowid", position, IntegerType.ROW_ID_TYPE, true);
//    }
//
//    TiColumnInfo copyWithoutPrimaryKey() {
//        TiColumnInfo.InternalTypeHolder typeHolder = this.type.toTypeHolder();
//        typeHolder.setFlag(this.type.getFlag() & -3);
//        DataType newType = DataTypeFactory.of(typeHolder);
//        return new TiColumnInfo(this.id, this.name, this.offset, newType, this.schemaState,
//                this.originDefaultValue, this.defaultValue, this.defaultValueBit, this.comment,
//                this.version, this.generatedExprString);
//    }
//
//    public long getId() {
//        return this.id;
//    }
    
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public int getOffset() {
        return offset;
    }
    
    public void setOffset(int offset) {
        this.offset = offset;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }
    
    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }
    
    public boolean matchName(String name) {
        return this.name.equalsIgnoreCase(name) ||
                String.format("`%s`", this.name).equalsIgnoreCase(name);
    }

//    ColumnInfo toProto(TiTableInfo tableInfo) {
//        return ColumnInfo.newBuilder()
//                .setColumnId(this.id)
//                .setTp(this.type.getTypeCode())
//                .setCollation(this.type.getCollationCode())
//                .setColumnLen((int) this.type.getLength())
//                .setDecimal(this.type.getDecimal())
//                .setFlag(this.type.getFlag())
//                .setDefaultVal(this.getOriginDefaultValueAsByteString())
//                .setPkHandle(tableInfo.isPkHandle() && this.isPrimaryKey())
//                .addAllElems(this.type.getElems())
//                .build();
//    }
    
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (!(other instanceof TiColumnInfo)) {
            return false;
        } else {
            TiColumnInfo col = (TiColumnInfo) other;
            return Objects.equals(this.name, col.name) &&
                    Objects.equals(this.type, col.type) &&
                    Objects.equals(this.offset, col.offset) &&
                    this.isPrimaryKey == col.isPrimaryKey;
        }
    }
    
    public int hashCode() {
        return Objects.hash(
                this.name, this.offset, this.type, this.isPrimaryKey);
    }
    
    
    @JsonIgnoreProperties(
            ignoreUnknown = true
    )
    public static class InternalTypeHolder {
        
        private int tp;
        private int flag;
        private long flen;
        private int decimal;
        private String charset;
        private String collate;
        private List<String> elems;
        
        @JsonCreator
        public InternalTypeHolder(@JsonProperty("Tp") int tp, @JsonProperty("Flag") int flag,
                @JsonProperty("Flen") long flen, @JsonProperty("Decimal") int decimal,
                @JsonProperty("Charset") String charset, @JsonProperty("Collate") String collate,
                @JsonProperty("Elems") List<String> elems) {
            this.tp = tp;
            this.flag = flag;
            this.flen = flen;
            this.decimal = decimal;
            this.charset = charset;
            this.collate = collate;
            this.elems = elems;
        }
        
        public int getTp() {
            return this.tp;
        }
        
        public void setTp(int tp) {
            this.tp = tp;
        }
        
        public int getFlag() {
            return this.flag;
        }
        
        public void setFlag(int flag) {
            this.flag = flag;
        }
        
        public long getFlen() {
            return this.flen;
        }
        
        public void setFlen(long flen) {
            this.flen = flen;
        }
        
        public int getDecimal() {
            return this.decimal;
        }
        
        public void setDecimal(int decimal) {
            this.decimal = decimal;
        }
        
        public String getCharset() {
            return this.charset;
        }
        
        public void setCharset(String charset) {
            this.charset = charset;
        }
        
        public String getCollate() {
            return this.collate;
        }
        
        public void setCollate(String collate) {
            this.collate = collate;
        }
        
        public List<String> getElems() {
            return this.elems;
        }
        
        public void setElems(List<String> elems) {
            this.elems = elems;
        }
    }
}