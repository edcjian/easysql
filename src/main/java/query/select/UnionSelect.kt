package query.select

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import com.alibaba.druid.sql.ast.statement.SQLUnionOperator
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import database.DBConnection
import dsl.alias
import dsl.count
import visitor.getDbType
import visitor.getQueryExpr
import java.sql.Connection

class UnionSelect(
    left: SelectQuery,
    operator: SQLUnionOperator,
    right: SelectQuery,
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) :
    SelectQueryImpl() {
    private var unionSelect = SQLUnionQuery()

    init {
        this.conn = left.conn
        unionSelect.dbType = getDbType(db)
        unionSelect.left = left.getSelect()
        unionSelect.operator = operator
        unionSelect.right = right.getSelect()
    }

    override fun sql(): String =
        SQLUtils.toSQLString(unionSelect, unionSelect.dbType, SQLUtils.FormatOption(), VisitorFeature.OutputNameQuote)

    override fun getSelect(): SQLSelectQuery {
        return this.unionSelect
    }

    override fun getDbType(): DB {
        return this.db
    }

    override fun fetchCount(): Long {
        val select = Select(this.db).select(count() alias "count").from(this).alias("t")
        val result = database.query(conn!!, select.sql())
        if (!isTransaction) {
            conn!!.close()
        }
        return result[0]["count"] as Long
    }
}