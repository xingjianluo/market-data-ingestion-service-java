package com.github.xingjianluo.marketdataingestionservicejava.journal;

import com.github.xingjianluo.marketdataingestionservicejava.model.NormalizedEvent;

import java.util.List;

/** 仅追加的事件日志，用于重启后重放恢复。 */
public interface EventJournal {

    /** 持久化一条已接受事件。 */
    void append(NormalizedEvent event);

    /** 按写入顺序读取全部事件（重启恢复用）。 */
    List<NormalizedEvent> replayAll();
}
