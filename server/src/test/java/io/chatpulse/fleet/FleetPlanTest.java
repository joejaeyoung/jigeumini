package io.chatpulse.fleet;

import io.chatpulse.discovery.SelectionPolicy.SourceMode;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FleetPlanTest {
    @Test void unofficialCollectionNeverExpandsPastThreeAndOfficialTransportIsNotImplied() {
        var channels=List.of("a".repeat(32),"b".repeat(32),"c".repeat(32),"d".repeat(32));
        assertThrows(IllegalArgumentException.class,()->new FleetPlan("a".repeat(64),SourceMode.UNOFFICIAL_POC,4,4,channels,30));
        assertThrows(IllegalArgumentException.class,()->new FleetPlan("a".repeat(64),SourceMode.OFFICIAL_AUTHORIZED,4,4,channels,30));
        assertDoesNotThrow(()->new FleetPlan("a".repeat(64),SourceMode.SYNTHETIC,4,4,channels,30));
    }
    @Test void duplicateChannelsAndUnboundedPlanLifetimesAreRejected() {
        assertThrows(IllegalArgumentException.class,()->new FleetPlan("a".repeat(64),SourceMode.SYNTHETIC,2,2,List.of("a".repeat(32),"a".repeat(32)),30));
        assertThrows(IllegalArgumentException.class,()->new FleetPlan("a".repeat(64),SourceMode.SYNTHETIC,0,0,List.of(),3601));
    }
}
