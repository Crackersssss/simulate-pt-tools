package com.cracker.pt.onlineschemachange.context;

import com.cracker.pt.core.database.DataSource;
import com.cracker.pt.onlineschemachange.statement.AlterStatement;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@RequiredArgsConstructor
public final class ExecuteDatasource {

    private final AlterStatement alterStatement;

    private ExecuteContext context;

    private final DataSource dataSource;
}
