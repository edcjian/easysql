package query.truncate

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.TableSchema
import visitor.getDbType
import java.sql.Connection

class Truncate(
    var db: DB = DB.MYSQL,
    private var conn: Connection? = null,
    private var isTransaction: Boolean = false
) {
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

    fun sql(): String {
        return SQLUtils.toSQLString(
            sqlTruncate,
            sqlTruncate.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }

    override fun toString(): String {
        return sql()
    }

    fun exec(): Int {
        val result = database.exec(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }
        return result
    }
}