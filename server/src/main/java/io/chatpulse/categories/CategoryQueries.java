package io.chatpulse.categories;

import io.chatpulse.quality.RankingStore;
import java.time.*;
import java.util.*;

/** 수집 품질을 통과한 채널의 합계와 참고 건수를 분리한다. */
public final class CategoryQueries {
    public record Channel(String channelId,Long count,long referenceCount,String quality) { }
    public record Entry(String categoryKey,String name,Long count,long referenceCount,int observedChannels,int healthyChannels,Double share,List<Channel> channels) { }
    public record Response(String asOf,String from,String to,String scope,String status,long revision,long unknownReferenceCount,boolean truncated,List<Entry> categories) { }
    public record Point(String from,String to,Long count,long referenceCount,int healthyChannels,int observedChannels) { }
    public record Timeline(String categoryKey,String name,String asOf,String from,String to,String status,long revision,String scope,boolean truncated,List<Point> points) { }
    private final CategoryStore store;
    private final RankingStore ranking;
    private final Clock clock;
    public CategoryQueries(CategoryStore store,RankingStore ranking,Clock clock) { this.store=store; this.ranking=ranking; this.clock=clock; }
    private Instant to() { return Instant.ofEpochSecond(Math.floorDiv(clock.instant().getEpochSecond()-8,60)*60); }
    public Response current() {
        Instant end=to(),start=end.minusSeconds(300); var window=store.window(start,end);
        var quality=new HashMap<String,Boolean>();
        for(String channel:window.channels()) {
            var points=ranking.activity(channel,start,end).points();
            quality.put(channel,window.fresh()&&points.size()==5&&points.stream().allMatch(p->p.status().equals("HEALTHY")));
        }
        var contributions=new TreeMap<String,Map<String,Long>>();
        window.memberships().forEach(member->contributions.computeIfAbsent(member.categoryKey(),k->new TreeMap<>()).put(member.channelId(),0L));
        for(var bucket:window.buckets())contributions.computeIfAbsent(bucket.categoryKey(),k->new TreeMap<>()).merge(bucket.channelId(),bucket.count(),Long::sum);
        long denominator=contributions.values().stream().flatMap(m->m.entrySet().stream()).filter(e->quality.getOrDefault(e.getKey(),false)).mapToLong(Map.Entry::getValue).sum();
        var entries=new ArrayList<Entry>();
        for(var category:contributions.entrySet()) {
            var channels=category.getValue().entrySet().stream().map(e->new Channel(e.getKey(),quality.getOrDefault(e.getKey(),false)?e.getValue():null,e.getValue(),quality.getOrDefault(e.getKey(),false)?"HEALTHY":"INCOMPLETE")).toList();
            int healthy=(int)channels.stream().filter(c->c.count()!=null).count(); long count=channels.stream().filter(c->c.count()!=null).mapToLong(Channel::count).sum();
            var label=window.labels().get(category.getKey());
            entries.add(new Entry(category.getKey(),label==null?"분류 이름 미확인":label.name(),healthy==0?null:count,
                    channels.stream().mapToLong(Channel::referenceCount).sum(),channels.size(),healthy,
                    healthy==0||denominator==0?null:(double)count/denominator,channels));
        }
        entries.sort(Comparator.comparing((Entry e)->e.count()==null?Long.MIN_VALUE:e.count()).reversed().thenComparing(Entry::categoryKey));
        long unknown=entries.stream().filter(e->e.categoryKey().equals(CategoryStore.UNKNOWN)).mapToLong(Entry::referenceCount).sum();
        return new Response(clock.instant().toString(),start.toString(),end.toString(),"COLLECTED_CHANNELS_ONLY",window.fresh()?"READY":"LAGGING",
                window.revision(),unknown,window.truncated(),List.copyOf(entries));
    }
    public Timeline timeline(String category) {
        if(!category.matches("[a-f0-9]{64}|unknown"))throw new IllegalArgumentException("CATEGORY_ID");
        Instant end=to(),start=end.minusSeconds(3600);var window=store.window(start,end);
        var quality=new HashMap<String,List<RankingStore.ActivityPoint>>();
        for(String channel:window.channels())quality.put(channel,ranking.activity(channel,start,end).points());
        var points=new ArrayList<Point>();
        for(int i=0;i<60;i++) {
            Instant from=start.plusSeconds(i*60L),to=from.plusSeconds(60);var counts=new HashMap<String,Long>();
            for(var member:window.memberships())if(member.categoryKey().equals(category)&&member.from().isBefore(to)&&member.to().isAfter(from))counts.put(member.channelId(),0L);
            for(var bucket:window.buckets())if(bucket.categoryKey().equals(category)&&!bucket.at().isBefore(from)&&bucket.at().isBefore(to))counts.merge(bucket.channelId(),bucket.count(),Long::sum);
            long included=0,reference=0;int healthy=0;
            for(var item:counts.entrySet()) {
                reference+=item.getValue();var q=quality.get(item.getKey());
                if(window.fresh()&&q!=null&&q.size()==60&&q.get(i).status().equals("HEALTHY")) { included+=item.getValue();healthy++; }
            }
            points.add(new Point(from.toString(),to.toString(),healthy==0?null:included,reference,healthy,counts.size()));
        }
        var label=window.labels().get(category);
        return new Timeline(category,label==null?"분류 이름 미확인":label.name(),clock.instant().toString(),start.toString(),end.toString(),window.fresh()?"READY":"LAGGING",window.revision(),"COLLECTED_CHANNELS_ONLY",window.truncated(),List.copyOf(points));
    }
}
