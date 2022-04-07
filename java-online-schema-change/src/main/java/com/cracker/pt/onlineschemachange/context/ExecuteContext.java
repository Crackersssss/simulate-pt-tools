package com.cracker.pt.onlineschemachange.context;

import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Execute context.
 */
@RequiredArgsConstructor
@Getter
@Setter
public final class ExecuteContext {

    private List<String> oldColumns = Collections.emptyList();

    private List<String> newColumns = Collections.emptyList();

    private AlterStatement alterStatement;

    private String newTableName;

    private String renameOldTableName;

    private String primaryKey;

    private String copyMinIndex;

    private String copyMaxIndex;

    private String copyStartIndex;

    private String copyEndIndex;

    private List<String> resultSetStartIndex = new ArrayList<>();

    private List<String> resultSetEndIndex = new ArrayList<>();

    private String deleteTrigger;

    private String updateTrigger;

    private String insertTrigger;

    private boolean isEnd;
}
