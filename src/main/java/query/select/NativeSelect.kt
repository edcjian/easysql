package query.select

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock
import expr.DB
import java.sql.Connection

class NativeSelect(
    var db: DB = DB.MYSQL,
    var sql: String,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : SelectQueryImpl() {
    override fun getSelect(): SQLSelectQuery {
        return SQLSelectQueryBlock()
    }

    override fun getDbType(): DB {
        return db
    }

    override fun sql(): String {
        return this.sql
    }
}