package io.chatpulse.pipeline;

import java.nio.file.*;
import java.util.Map;
import tools.jackson.databind.json.JsonMapper;

/** 부하·전용 broker 실험에서 공통으로 사용하는 정제 결과 렌더러. */
final class ExperimentReport {
    private static final JsonMapper JSON=JsonMapper.builder().build();
    static void writeHtml(Path output,Map<String,Object> result) throws Exception {
        String pretty=JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");
        String html="<!doctype html><html lang=ko><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>지금이니 합성 부하 실험</title><style>body{max-width:1100px;margin:40px auto;padding:0 20px;font:16px/1.7 system-ui;color:#173c30}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f0f7f4;padding:20px}h1{font-size:28px}summary{cursor:pointer}</style><h1>합성 부하 실험</h1><p>목표 수치와 실제 결과를 구분합니다. 단일 broker·RF1이며 외부 플랫폼에 요청하지 않았습니다.</p><h2>"+result.get("outcome")+"</h2><details open><summary>환경·입력·계수·지연·partition·자원·검증 결과</summary><pre>"+pretty+"</pre></details></html>";
        Files.writeString(output.resolveSibling(output.getFileName()+".html"),html,StandardOpenOption.CREATE_NEW);
    }
}
