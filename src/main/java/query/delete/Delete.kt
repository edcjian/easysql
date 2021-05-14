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

class Delete(db: DB = DB.MYSQL) {
    private var sqlDelete = SQLDeleteStatement()

    private var dbType: DB = db

    init {
        sqlDelete.dbType = getDbType(dbType)
    }

    infix fun from(table: String): Delete {
        sqlDelete.tableSource = SQLExprTableSource(table)
        return this
    }

    infix fun <T : TableSchema> from(table: T): Delete {
        return from(table.tableName)
    }

    infix fun where(condition: Query): Delete {
        this.sqlDelete.addCondition(getQueryExpr(condition, this.dbType).expr)
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
}