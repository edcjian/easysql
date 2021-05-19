package query.truncate

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement
import com.alibaba.druid.sql.visitor.VisitorFeature
import database.DBConnection
import expr.DB
import expr.TableSchema
import visitor.getDbType

class Truncate(var db: DB = DB.MYSQL, var dataSource: DBConnection? = null) {
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
        val conn = this.dataSource!!.getDataSource().connection
        return database.exec(conn, this.sql())
    }
}