package query.select

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import com.alibaba.druid.sql.ast.statement.SQLUnionOperator
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import database.DBConnection
import visitor.getDbType

class UnionSelect(
    left: SelectQuery,
    operator: SQLUnionOperator,
    right: SelectQuery,
    var db: DB = DB.MYSQL,
    override var dataSource: DBConnection? = null
) :
    SelectQueryImpl() {
    private var unionSelect = SQLUnionQuery()

    init {
        this.dataSource = left.dataSource
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
}