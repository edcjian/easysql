package query.ddl

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import query.ReviseQuery
import visitor.getDbType
import java.sql.Connection

class CreateIndex(
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : ReviseQuery() {
    private var sqlCreateIndex = SQLCreateIndexStatement()

    init {
        sqlCreateIndex.dbType = getDbType(db)
    }

    infix fun create(indexName: String): CreateIndex {
        sqlCreateIndex.name = SQLIdentifierExpr(indexName)
        return this
    }

    infix fun createUnique(indexName: String): CreateIndex {
        sqlCreateIndex.name = SQLIdentifierExpr(indexName)
        sqlCreateIndex.type = "UNIQUE"
        return this
    }

    infix fun on(table: String): CreateIndex {
        sqlCreateIndex.table = SQLExprTableSource(table)
        return this
    }

    infix fun column(column: String): CreateIndex {
        sqlCreateIndex.addItem(SQLSelectOrderByItem(SQLIdentifierExpr(column)))
        return this
    }

    infix fun column(columns: List<String>): CreateIndex {
        columns.forEach {
            sqlCreateIndex.addItem(SQLSelectOrderByItem(SQLIdentifierExpr(it)))
        }
        return this
    }

    override fun sql(): String {
        return SQLUtils.toSQLString(
            sqlCreateIndex,
            sqlCreateIndex.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }
}