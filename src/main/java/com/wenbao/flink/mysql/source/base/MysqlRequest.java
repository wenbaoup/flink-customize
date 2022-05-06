package com.wenbao.flink.mysql.source.base;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * MysqlRequest
 *
 * @author wenbao
 * @since 2022/03/10
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class MysqlRequest {
    private TiTableInfo tableInfo;
    private List<String> requiredCols;
    private String expression;
    
    
}