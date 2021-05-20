package query.select

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import expr.DB
import query.BasedQuery
import java.sql.Connection

interface SelectQuery : BasedQuery {
    fun getSelect(): SQLSelectQuery

    fun getDbType(): DB
}