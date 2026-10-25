package io.chatpulse.fleet;

import java.nio.file.Files;
import java.nio.file.Path;

/** System.exit 없이 main을 반환해 소유한 non-daemon 실행기가 남으면 프로세스 종료가 막히게 한다. */
public final class FleetRuntimeProcess {
    public static void main(String[] args) throws Exception {
        int code;
        try { code = FleetCli.execute(args, System.getenv()); }
        catch (Exception failure) { code = 2; }
        Files.writeString(Path.of(System.getenv("FLEET_TEST_RETURNED")), Integer.toString(code));
    }
}
