package io.chatpulse.categories;

import io.chatpulse.quality.RankingStore;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class CategoryQueriesTest {
    private static final String A="a".repeat(32),B="b".repeat(32),GAME=CategoryStore.key("game");
    private static final Instant END=Instant.parse("2026-10-25T12:00:00Z"),START=END.minusSeconds(300);
    private final CategoryStore store=mock(CategoryStore.class);
    private final RankingStore ranking=mock(RankingStore.class);
    private final CategoryQueries queries=new CategoryQueries(store,ranking,Clock.fixed(END.plusSeconds(8),ZoneOffset.UTC));
    private RankingStore.ActivityResponse quality(String channel,Instant start,int length,boolean healthy) {
        var points=new ArrayList<RankingStore.ActivityPoint>();
        for(int i=0;i<length;i++)points.add(new RankingStore.ActivityPoint(start.plusSeconds(i*60L).toString(),start.plusSeconds((i+1)*60L).toString(),healthy?0L:null,0,healthy?1:0,healthy?"HEALTHY":"INCOMPLETE",List.of()));
        return new RankingStore.ActivityResponse(channel,END.toString(),start.toString(),END.toString(),points);
    }
    private CategoryStore.Window window(boolean fresh) {
        return new CategoryStore.Window(List.of(A,B),List.of(new CategoryStore.Bucket(A,GAME,START,6),new CategoryStore.Bucket(A,"unknown",START,4),new CategoryStore.Bucket(B,GAME,START,100)),
                Map.of(GAME,new CategoryStore.Category(GAME,"합성 게임"),"unknown",new CategoryStore.Category("unknown","카테고리 미상")),
                List.of(new CategoryStore.Membership(A,GAME,START,END),new CategoryStore.Membership(B,GAME,START,END)),7,false,fresh);
    }
    @Test void fiveMinutesIncludesHealthyCountsAndSeparatesUnknownAndExcludedReferences() {
        when(store.window(START,END)).thenReturn(window(true));
        when(ranking.activity(A,START,END)).thenReturn(quality(A,START,5,true));when(ranking.activity(B,START,END)).thenReturn(quality(B,START,5,false));
        var response=queries.current();var game=response.categories().getFirst();
        assertEquals(6L,game.count());assertEquals(106,game.referenceCount());assertEquals(2,game.observedChannels());assertEquals(1,game.healthyChannels());assertEquals(.6,game.share());
        assertEquals(4,response.unknownReferenceCount());assertNull(game.channels().get(1).count());assertEquals(7,response.revision());
        assertEquals("COLLECTED_CHANNELS_ONLY",response.scope());verify(store).window(START,END);
        when(store.window(START,END)).thenReturn(window(false));
        assertTrue(queries.current().categories().stream().allMatch(e->e.count()==null&&e.share()==null));assertEquals("LAGGING",queries.current().status());
    }
    @Test void quietMembershipIsZeroButMissingCoverageAndAbsentMembershipAreGaps() {
        Instant start=END.minusSeconds(3600);
        var quiet=new CategoryStore.Window(List.of(A),List.of(),Map.of(GAME,new CategoryStore.Category(GAME,"합성 게임")),
                List.of(new CategoryStore.Membership(A,GAME,start,start.plusSeconds(60))),2,false,true);
        when(store.window(start,END)).thenReturn(quiet);when(ranking.activity(A,start,END)).thenReturn(quality(A,start,60,true));
        var timeline=queries.timeline(GAME);assertEquals(60,timeline.points().size());assertEquals(0L,timeline.points().getFirst().count());
        assertNull(timeline.points().get(1).count());assertEquals(0,timeline.points().get(1).observedChannels());
        when(ranking.activity(A,start,END)).thenReturn(quality(A,start,60,false));assertNull(queries.timeline(GAME).points().getFirst().count());
        when(store.window(START,END)).thenReturn(new CategoryStore.Window(List.of(A),List.of(),quiet.labels(),List.of(new CategoryStore.Membership(A,GAME,START,END)),2,false,true));
        when(ranking.activity(A,START,END)).thenReturn(quality(A,START,5,true));var zero=queries.current().categories().getFirst();
        assertEquals(0L,zero.count());assertNull(zero.share());
    }
    @Test void httpPreservesNullZeroCachePolicyAndSanitizesFailures() throws Exception {
        var queries=mock(CategoryQueries.class);var consumer=mock(CategoryConsumer.class);
        var mvc=MockMvcBuilders.standaloneSetup(new CategoryController(queries,consumer)).build();
        when(queries.current()).thenReturn(new CategoryQueries.Response(END.toString(),START.toString(),END.toString(),"COLLECTED_CHANNELS_ONLY","READY",0,0,false,List.of()));
        mvc.perform(get("/api/v1/categories")).andExpect(status().isOk()).andExpect(header().string("Cache-Control","no-store")).andExpect(jsonPath("$.categories").isEmpty());
        mvc.perform(get("/api/v1/categories/bad/timeline")).andExpect(status().isBadRequest());verify(queries,never()).timeline(anyString());
        when(queries.current()).thenThrow(new DataAccessResourceFailureException("SYNTHETIC_PRIVATE_SQL"));
        mvc.perform(get("/api/v1/categories")).andExpect(status().isServiceUnavailable()).andExpect(content().json("{\"reason\":\"QUERY_UNAVAILABLE\"}"));
    }
}
