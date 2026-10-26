package io.chatpulse.retention;

import java.util.Map;

/** 합계를 구해도 각 삭제 단계가 한도에 도달했는지 잃지 않는다. 잔여 행 수를 뜻하지는 않는다. */
public record CleanupBatch(int deletedRows,boolean limited) {
    public static CleanupBatch empty() { return new CleanupBatch(0,false); }
    public CleanupBatch add(int rows,int limit) { return new CleanupBatch(deletedRows+rows,limited||rows>=limit); }
    public Map<String,Object> summary() { return Map.of("deletedRows",deletedRows,"limited",limited); }
}
