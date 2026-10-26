package io.chatpulse.fleet;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** 소유 loopback 소켓에만 단절·전달 차단을 주입한다. 차단 중 bytes를 복구 버퍼로 보관하지 않는다. */
final class ControlledTcpProxy implements AutoCloseable {
    enum Mode { FORWARD, CUT, BLACKHOLE }
    private final ServerSocket listener;
    private final int targetPort;
    private final Set<Socket> sockets=new HashSet<>();
    private final ExecutorService workers=Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicLong forwarded=new AtomicLong(),discarded=new AtomicLong();
    private volatile Mode mode=Mode.FORWARD;private volatile boolean closed;
    ControlledTcpProxy(int targetPort) throws Exception {
        this.targetPort=targetPort;listener=new ServerSocket(0,20,InetAddress.getLoopbackAddress());
        workers.submit(()->{while(!closed)try {accept(listener.accept());}catch(Exception failure){if(!closed)throw new IllegalStateException("PROXY_ACCEPT_FAILED");}});
    }
    int port(){return listener.getLocalPort();}
    long forwardedBytes(){return forwarded.get();}
    long discardedBytes(){return discarded.get();}
    synchronized void mode(Mode value) {
        mode=value;if(value==Mode.CUT){for(Socket socket:sockets)close(socket);sockets.clear();}
    }
    private void accept(Socket client) throws Exception {
        if(mode==Mode.CUT||closed){close(client);return;}
        Socket target;
        try {target=new Socket();target.connect(new InetSocketAddress("127.0.0.1",targetPort),1000);}
        catch(Exception failure){close(client);return;}
        synchronized(this) {
            if(mode==Mode.CUT||closed){close(client);close(target);return;}
            sockets.add(client);sockets.add(target);
            workers.submit(()->copy(client,target));workers.submit(()->copy(target,client));
        }
    }
    private void copy(Socket from,Socket to) {
        try {
            byte[] bytes=new byte[8192];int count;
            while((count=from.getInputStream().read(bytes))!=-1) {
                if(mode==Mode.FORWARD){to.getOutputStream().write(bytes,0,count);to.getOutputStream().flush();forwarded.addAndGet(count);}
                else if(mode==Mode.BLACKHOLE)discarded.addAndGet(count);
                else break;
            }
        }catch(Exception ignored){}
        finally {close(from);close(to);synchronized(this){sockets.remove(from);sockets.remove(to);}}
    }
    private static void close(Socket socket){try{socket.close();}catch(Exception ignored){}}
    @Override public void close() throws Exception {
        closed=true;listener.close();mode(Mode.CUT);workers.shutdownNow();
        if(!workers.awaitTermination(5,TimeUnit.SECONDS))throw new IllegalStateException("PROXY_STOP_TIMEOUT");
    }
}
