package io.chatpulse.quality;

import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class RankingControllerTest {
    @Test void rejectsInvalidWindowsLimitsCursorsAndUnfinishedActivityBeforeQuery() throws Exception {
        var store=mock(RankingStore.class);var mvc=MockMvcBuilders.standaloneSetup(new RankingController(store)).build();
        mvc.perform(get("/api/v1/rankings").param("window","10")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/rankings").param("limit","101")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/spikes").param("cursor","invalid")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/spikes").param("limit","0")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/channels/invalid/activity")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/channels/"+"a".repeat(32)+"/activity").param("to",Instant.now().plusSeconds(600).toString())).andExpect(status().isBadRequest());
        verifyNoInteractions(store);
    }
    @Test void warmupAndDbUnavailableAreDistinctAndDoNotExposeSql() throws Exception {
        var store=mock(RankingStore.class);var mvc=MockMvcBuilders.standaloneSetup(new RankingController(store)).build();
        when(store.rankings(60,20)).thenReturn(new RankingStore.RankingResponse(Instant.now().toString(),null,null,60,RankingStore.VERSION,"WARMING_UP",List.of(),List.of()));
        mvc.perform(get("/api/v1/rankings")).andExpect(status().isOk()).andExpect(header().string("Cache-Control","no-store")).andExpect(jsonPath("$.status").value("WARMING_UP"));
        when(store.rankings(60,20)).thenThrow(new DataAccessResourceFailureException("SYNTHETIC_PRIVATE_SQL"));
        mvc.perform(get("/api/v1/rankings")).andExpect(status().isServiceUnavailable()).andExpect(content().json("{\"reason\":\"QUERY_UNAVAILABLE\"}"));
    }
    @Test void validFeedCursorIsParsedIntoFixedPosition() throws Exception {
        var store=mock(RankingStore.class);var mvc=MockMvcBuilders.standaloneSetup(new RankingController(store)).build();
        var at=Instant.parse("2026-10-25T00:00:00Z");var id=UUID.randomUUID();
        when(store.spikes(at,id,2)).thenReturn(new RankingStore.SpikeResponse(at.toString(),"READY",List.of(),null));
        mvc.perform(get("/api/v1/spikes").param("cursor",at+"|"+id).param("limit","2")).andExpect(status().isOk()).andExpect(jsonPath("$.events").isEmpty());
        verify(store).spikes(at,id,2);
    }
}
