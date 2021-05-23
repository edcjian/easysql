package query.ddl

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.TableSchema
import query.ReviseQuery
import visitor.getDbType
import java.sql.Connection

class DropTable(
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : ReviseQuery() {
    private var sqlDropTable = SQLDropTableStatement()

    init {
        sqlDropTable.dbType = getDbType(db)
    }

    infix fun drop(table: String): DropTable {
        sqlDropTable.setName(SQLIdentifierExpr(table))
        return this
    }

    override fun sql(): String {
        return SQLUtils.toSQLString(
            sqlDropTable,
            sqlDropTable.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }
}