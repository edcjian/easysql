package query.select

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import expr.DB
import jdbc.DataSource

interface SelectQuery {
    var dataSource: DataSource?

    fun sql(): String

    fun getSelect(): SQLSelectQuery

    fun getDbType(): DB

    fun query(): List<Map<String, Any>>
}