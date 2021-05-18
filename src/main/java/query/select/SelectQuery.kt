package query.select

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import expr.DB
import database.DBConnection

interface SelectQuery {
    var dataSource: DBConnection?

    fun sql(): String

    fun getSelect(): SQLSelectQuery

    fun getDbType(): DB
}