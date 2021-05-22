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

    private var records = mutableListOf<MutableMap<String, Any>>()

    init {
        sqlInsert.dbType = getDbType(db)
    }

    infix fun into(table: String): NativeInsert {
        sqlInsert.tableSource = SQLExprTableSource(table)
        return this
    }

    infix fun value(value: MutableMap<String, Any>): NativeInsert {
        if (columns.isEmpty()) {
            value.forEach {
                sqlInsert.addColumn(SQLIdentifierExpr(it.key))
                columns.add(it.key)
            }
        }

        val valuesClause = SQLInsertStatement.ValuesClause()

        columns.forEach { col ->
            value[col]?.let { valuesClause.addValue(getExpr(it)) }
        }
        sqlInsert.addValueCause(valuesClause)

        records.add(value)

        return this
    }

    infix fun values(values: List<MutableMap<String, Any>>): NativeInsert {
        values.forEach {
            value(it)
        }

        return this
    }

    fun values(vararg values: MutableMap<String, Any>): NativeInsert {
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
        val result = database.execReturnKey(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }

        result.forEachIndexed { index, item ->
            records[index]["incrKey"] = item
        }
        return records.size
    }
}