package com.wenbao.flink.mysql.source.cdc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CanalJsonEvent
 *
 * @author wenbao
 * @since 2022/03/16
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CanalJsonEvent {
    
    private String type;
    private long es;
    private long ts;
    private String data;
    private String old;
}