package query.ddl

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.TableSchema
import query.ReviseQuery
import visitor.getDbType
import java.sql.Connection

class DropIndex(
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : ReviseQuery() {
    private var sqlDropIndex = SQLDropIndexStatement()

    init {
        sqlDropIndex.dbType = getDbType(db)
    }

    infix fun drop(indexName: String): DropIndex {
        sqlDropIndex.indexName = SQLIdentifierExpr(indexName)
        return this
    }

    infix fun on(table: String): DropIndex {
        sqlDropIndex.tableName = SQLExprTableSource(table)
        return this
    }

    override fun sql(): String {
        return SQLUtils.toSQLString(
            sqlDropIndex,
            sqlDropIndex.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }
}