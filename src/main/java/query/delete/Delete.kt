package query.delete

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.Query
import expr.TableSchema
import database.DBConnection
import visitor.getDbType
import visitor.getQueryExpr

class Delete(var db: DB = DB.MYSQL, var dataSource: DBConnection? = null) {
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

    fun exec(): Int {
        val conn = this.dataSource!!.getDataSource().connection
        return database.exec(conn, this.sql())
    }
}