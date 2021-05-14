package query.select

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery
import com.alibaba.druid.sql.ast.statement.SQLUnionOperator
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery
import com.alibaba.druid.sql.visitor.VisitorFeature
import visitor.getDbType
import expr.DB

class UnionSelect(
    left: SelectQuery,
    operator: SQLUnionOperator,
    right: SelectQuery,
    db: DB = DB.MYSQL
) :
    SelectQuery {
    private var unionSelect = SQLUnionQuery()

    private var dbType: DB = db

    init {
        unionSelect.dbType = getDbType(dbType)
        unionSelect.left = left.getSelect()
        unionSelect.operator = operator
        unionSelect.right = right.getSelect()
    }

    override fun sql(): String = SQLUtils.toSQLString(unionSelect, unionSelect.dbType, SQLUtils.FormatOption(), VisitorFeature.OutputNameQuote)

    override fun getSelect(): SQLSelectQuery {
        return this.unionSelect
    }

    override fun getDbType(): DB {
        return this.dbType
    }
}