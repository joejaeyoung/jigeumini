package io.chatpulse.observations;

import io.chatpulse.quality.RankingStore;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ObservationControllerTest {
    @Test void nonemptyServerSerializationMatchesSharedWebContractFixture() throws Exception {
        String fixture;
        try(var input=getClass().getResourceAsStream("/observation-timeline.json")) {
            fixture=new String(java.util.Objects.requireNonNull(input).readAllBytes(),java.nio.charset.StandardCharsets.UTF_8);
        }
        var expected=tools.jackson.databind.json.JsonMapper.builder().build().readValue(fixture,ObservationController.Timeline.class);
        var observations=mock(ObservationStore.class);var ranking=mock(RankingStore.class);var consumer=mock(ObservationConsumer.class);
        Instant from=Instant.parse(expected.from()),to=Instant.parse(expected.to());
        when(observations.history(expected.channelId(),from,to)).thenReturn(new ObservationStore.History(expected.observations(),false));
        when(ranking.activity(expected.channelId(),from,to)).thenReturn(new RankingStore.ActivityResponse(expected.channelId(),expected.asOf(),expected.from(),expected.to(),expected.chat()));
        var mvc=MockMvcBuilders.standaloneSetup(new ObservationController(observations,ranking,consumer,Clock.fixed(Instant.parse(expected.asOf()),ZoneOffset.UTC))).build();
        mvc.perform(get("/api/v1/channels/"+expected.channelId()+"/timeline")).andExpect(status().isOk()).andExpect(content().json(fixture));
    }
    @Test void timelineUsesOneCompletedMinuteWindowAndRejectsInvalidInput() throws Exception {
        var observations = mock(ObservationStore.class); var ranking = mock(RankingStore.class); var consumer = mock(ObservationConsumer.class);
        var at = Instant.parse("2026-10-25T12:00:59Z"); var end = Instant.parse("2026-10-25T12:00:00Z"); var start = end.minusSeconds(3600);
        String id = "a".repeat(32), path = "/api/v1/channels/" + id + "/timeline";
        var mvc = MockMvcBuilders.standaloneSetup(new ObservationController(observations, ranking, consumer, Clock.fixed(at, ZoneOffset.UTC))).build();
        when(ranking.activity(id, start, end)).thenReturn(new RankingStore.ActivityResponse(id, at.toString(), start.toString(), end.toString(), List.of()));
        when(observations.history(id, start, end)).thenReturn(new ObservationStore.History(List.of(), false));
        mvc.perform(get(path)).andExpect(status().isOk()).andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.to").value(end.toString())).andExpect(jsonPath("$.observations").isEmpty());
        verify(ranking).activity(id, start, end); verify(observations).history(id, start, end);
        mvc.perform(get(path).param("seconds", "60")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/v1/channels/invalid/timeline")).andExpect(status().isBadRequest());
        verifyNoMoreInteractions(observations, ranking);
        when(ranking.activity(id, start, end)).thenThrow(new DataAccessResourceFailureException("SYNTHETIC_PRIVATE_SQL"));
        mvc.perform(get(path)).andExpect(status().isServiceUnavailable()).andExpect(content().json("{\"reason\":\"QUERY_UNAVAILABLE\"}"));
    }
}
