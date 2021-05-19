package query.select

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import expr.DB
import java.sql.Connection

interface SelectQuery {
    var conn: Connection?

    fun sql(): String

    fun getSelect(): SQLSelectQuery

    fun getDbType(): DB
}