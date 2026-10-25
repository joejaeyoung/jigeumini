package io.chatpulse.replay;

final class ReplayBuild {
    private static final String ID=read();
    private ReplayBuild() { }
    static String id() { return ID; }
    private static String read() {
        try(var input=ReplayBuild.class.getResourceAsStream("/replay-build-id.txt")) {
            String value=input==null?"":new String(input.readNBytes(128),java.nio.charset.StandardCharsets.UTF_8).strip();
            if(!value.matches("[a-f0-9]{64}"))throw new IllegalStateException();return value;
        } catch(Exception ignored) { throw new IllegalStateException("REPLAY_BUILD_UNAVAILABLE"); }
    }
}
