package io.chatpulse.metadata;

import io.chatpulse.pipeline.ProjectionStore;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static io.chatpulse.metadata.BroadcastMetadata.*;

class MetadataControllerTest {
    @Test void directoryRequestedAndDetailContractsAreTypedAndNotBrowserCached() throws Exception {
        String id = "a".repeat(32);
        var store = mock(ProjectionStore.class); var metadata = mock(MetadataService.class);
        when(store.recentChannelIds(20)).thenReturn(List.of(id));
        when(metadata.get(id)).thenReturn(empty(id, LiveStatus.UNKNOWN, Reason.PENDING));
        var mvc = MockMvcBuilders.standaloneSetup(new MetadataController(metadata, store)).build();
        mvc.perform(get("/api/channels")).andExpect(status().isOk()).andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.scope").value("COLLECTED_CHANNELS_ONLY")).andExpect(jsonPath("$.channels[0].channelId").value(id));
        mvc.perform(get("/api/channels").param("ids", id + "," + id)).andExpect(status().isOk())
                .andExpect(jsonPath("$.channels.length()").value(1)).andExpect(jsonPath("$.scope").value("REQUESTED_CHANNELS"));
        mvc.perform(get("/api/channels/" + id)).andExpect(status().isOk()).andExpect(jsonPath("$.reason").value("PENDING"));
        verify(store, times(1)).recentChannelIds(20);
    }
    @Test void badIdsAndOverLimitNeverReachUpstreamOrStore() throws Exception {
        var store = mock(ProjectionStore.class); var metadata = mock(MetadataService.class);
        var mvc = MockMvcBuilders.standaloneSetup(new MetadataController(metadata, store)).build();
        for (String ids : List.of("", "bad", "A".repeat(32), "a".repeat(32) + ",", String.join(",", java.util.Collections.nCopies(21, "a".repeat(32))))) {
            mvc.perform(get("/api/channels").param("ids", ids)).andExpect(status().isBadRequest());
        }
        mvc.perform(get("/api/channels/invalid")).andExpect(status().isBadRequest());
        verifyNoInteractions(metadata, store);
    }
}
