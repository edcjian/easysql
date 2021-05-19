package query.delete

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.Query
import expr.TableSchema
import visitor.getDbType
import visitor.getQueryExpr
import java.sql.Connection

class Delete(var db: DB = DB.MYSQL, private var conn: Connection? = null, private var isTransaction: Boolean = false) {
    private var sqlDelete = SQLDeleteStatement()

    init {
        sqlDelete.dbType = getDbType(db)
    }

    infix fun from(table: String): Delete {
        sqlDelete.tableSource = SQLExprTableSource(table)
        return this
    }

    infix fun <T : TableSchema> from(table: T): Delete {
        return from(table.tableName)
    }

    infix fun where(condition: Query): Delete {
        this.sqlDelete.addCondition(getQueryExpr(condition, this.db).expr)
        return this
    }

    fun where(test: () -> Boolean, condition: Query): Delete {
        if (test()) {
            where(condition)
        }

        return this
    }

    fun where(test: Boolean, condition: Query): Delete {
        if (test) {
            where(condition)
        }

        return this
    }

    fun sql(): String {
        return SQLUtils.toSQLString(
            sqlDelete,
            sqlDelete.dbType,
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