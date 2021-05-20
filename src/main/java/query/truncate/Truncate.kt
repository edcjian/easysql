package query.truncate

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.TableSchema
import query.ReviseQuery
import visitor.getDbType
import java.sql.Connection

class Truncate(
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : ReviseQuery() {
    private var sqlTruncate = SQLTruncateStatement()

    init {
        sqlTruncate.dbType = getDbType(db)
    }

    infix fun truncate(table: String): Truncate {
        if (db == DB.DB2) {
            sqlTruncate.isImmediate = true
        }
        sqlTruncate.addTableSource(SQLIdentifierExpr(table))
        return this
    }

    infix fun <T : TableSchema> truncate(table: T): Truncate {
        return truncate(table.tableName)
    }

    override fun sql(): String {
        return SQLUtils.toSQLString(
            sqlTruncate,
            sqlTruncate.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }
}