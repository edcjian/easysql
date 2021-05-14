package query.select

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import expr.DB

interface SelectQuery {
    fun sql(): String

    fun getSelect(): SQLSelectQuery

    fun getDbType(): DB
}