package io.chatpulse.notifications;

import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class NotificationControllerTest {
    @Test void invalidFiltersAndCursorFailBeforeDbAccess() throws Exception {
        var store=mock(NotificationStore.class);var mvc=MockMvcBuilders.standaloneSetup(new NotificationController(store,mock(SpikeOutbox.class),mock(NotificationConsumer.class))).build();
        mvc.perform(get("/api/v1/notifications").param("channels","bad")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/notifications").param("limit","101")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/notifications").param("cursor","bad")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/notifications").param("cursor","9999-01-01T00:00:00Z|"+UUID.randomUUID())).andExpect(status().isBadRequest());
        verifyNoInteractions(store);
    }
    @Test void fixedCursorAndDbFailureAreSafe() throws Exception {
        var store=mock(NotificationStore.class);var mvc=MockMvcBuilders.standaloneSetup(new NotificationController(store,mock(SpikeOutbox.class),mock(NotificationConsumer.class))).build();
        var at=Instant.parse("2026-10-25T00:00:00Z");var id=UUID.randomUUID();var channels=List.of("a".repeat(32));
        when(store.page(channels,at,id,2)).thenReturn(new NotificationStore.Page(at.toString(),List.of(),null));
        mvc.perform(get("/api/v1/notifications").param("channels",channels.getFirst()).param("cursor",at+"|"+id).param("limit","2"))
                .andExpect(status().isOk()).andExpect(header().string("Cache-Control","no-store")).andExpect(jsonPath("$.events").isEmpty());
        when(store.page(anyList(),any(),any(),anyInt())).thenThrow(new DataAccessResourceFailureException("SYNTHETIC_PRIVATE_SQL"));
        mvc.perform(get("/api/v1/notifications")).andExpect(status().isServiceUnavailable()).andExpect(content().json("{\"reason\":\"QUERY_UNAVAILABLE\"}"));
    }
}
