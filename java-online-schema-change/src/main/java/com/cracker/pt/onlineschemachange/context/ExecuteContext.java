package com.cracker.pt.onlineschemachange.context;

import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Collections;
import java.util.List;

@RequiredArgsConstructor
@Getter
@Setter
public final class ExecuteContext {

    private List<String> oldColumns = Collections.emptyList();

    private List<String> newColumns = Collections.emptyList();

    private AlterStatement alterStatement;

    private String newTableName;

    private String renameOldTableName;
}