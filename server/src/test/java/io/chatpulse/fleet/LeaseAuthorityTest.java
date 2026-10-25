package io.chatpulse.fleet;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import io.chatpulse.poc.SourceFailure;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LeaseAuthorityTest {
    private static final String CHANNEL="a".repeat(32);
    private CollectorLeases.Grant grant(long epoch){return new CollectorLeases.Grant(Instant.EPOCH,List.of(new CollectorLeases.Lease(CHANNEL,epoch,Instant.EPOCH.plusSeconds(10),"version")));}
    @Test void networkTimeIsDeductedAndPermitExpiresWithoutHeartbeatThread() {
        var time=new AtomicLong(3_000_000_000L);var authority=new LeaseAuthority(time::get);authority.update(grant(1),0);
        var permit=authority.acquire(CHANNEL);time.set(7_999_999_999L);assertDoesNotThrow(permit::check);time.set(8_000_000_000L);assertThrows(SourceFailure.class,permit::check);
        assertThrows(SourceFailure.class,()->authority.update(grant(1),time.get()));
    }
    @Test void responseArrivingAfterItsLocalDeadlineCannotResurrectAuthority() {
        var authority=new LeaseAuthority(()->9_000_000_000L);assertThrows(SourceFailure.class,()->authority.update(grant(1),0));assertThrows(SourceFailure.class,authority::checkAll);
    }
    @Test void renewKeepsSameEpochPermitButRevocationAndReassignmentCannotReuseIt() {
        var time=new AtomicLong();var authority=new LeaseAuthority(time::get);authority.update(grant(1),0);var permit=authority.acquire(CHANNEL);
        time.set(5_000_000_000L);authority.update(grant(1),time.get());time.set(10_000_000_000L);assertDoesNotThrow(permit::check);
        assertThrows(SourceFailure.class,()->authority.update(grant(2),time.get()));assertThrows(SourceFailure.class,permit::check);
    }
    @Test void databaseFailureOrMissingLeaseCancelsPreviouslyIssuedPermit() {
        var authority=new LeaseAuthority(()->0);authority.update(grant(1),0);var permit=authority.acquire(CHANNEL);authority.revoke();assertThrows(SourceFailure.class,permit::check);
        var other=new LeaseAuthority(()->0);other.update(grant(1),0);assertThrows(SourceFailure.class,()->other.update(new CollectorLeases.Grant(Instant.EPOCH,List.of()),0));
    }
}
