package query.insert

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import query.ReviseQuery
import visitor.getDbType
import visitor.getExpr
import java.sql.Connection

class NativeInsert(
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : ReviseQuery() {
    private var sqlInsert = SQLInsertStatement()

    private var columns = mutableListOf<String>()

    init {
        sqlInsert.dbType = getDbType(db)
    }

    infix fun into(table: String): NativeInsert {
        sqlInsert.tableSource = SQLExprTableSource(table)
        return this
    }

    infix fun columns(columns: List<String>): NativeInsert {
        columns.forEach {
            this.columns.add(it)
            this.sqlInsert.addColumn(SQLIdentifierExpr(it))
        }

        return this
    }

    fun columns(vararg columns: String): NativeInsert {
        return columns(columns.toList())
    }

    infix fun value(value: Map<String, Any>): NativeInsert {
        val valuesClause = SQLInsertStatement.ValuesClause()

        columns.forEach { col ->
            value[col]?.let { valuesClause.addValue(getExpr(it)) }
        }
        sqlInsert.addValueCause(valuesClause)

        return this
    }

    infix fun values(values: List<Map<String, Any>>): NativeInsert {
        values.forEach {
            value(it)
        }

        return this
    }

    fun values(vararg values: Map<String, Any>): NativeInsert {
        return values(values.toList())
    }

    override fun sql(): String {
        return SQLUtils.toSQLString(
            sqlInsert,
            sqlInsert.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }

    override fun exec(): Int {
        // TODO 添加返回自增主键，测试没有自增主键的情况
        val result = database.execReturnKey(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }
        return this.sqlInsert.valuesList.size
    }
}