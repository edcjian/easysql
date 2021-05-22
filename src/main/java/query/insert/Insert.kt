package query.insert

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.QueryTableColumn
import expr.TableSchema
import query.ReviseQuery
import visitor.getDbType
import visitor.getExpr
import java.sql.Connection
import kotlin.reflect.full.declaredMemberProperties

class Insert(
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : ReviseQuery() {
    private var sqlInsert = SQLInsertStatement()

    private var columns = mutableListOf<String>()

    init {
        sqlInsert.dbType = getDbType(db)
    }

    infix fun <T : TableSchema> into(table: T): Insert {
        sqlInsert.tableSource = SQLExprTableSource(table.tableName)

        val clazz = table::class
        val declaredMemberProperties = clazz.declaredMemberProperties

        val properties = declaredMemberProperties.map { it.name to it.getter.call(table) }
            .filter { it.second is QueryTableColumn }
            .map { it.first to it.second as QueryTableColumn }
            .filter { it.second.incr == false }

        properties.forEach {
            columns.add(it.first)
            sqlInsert.addColumn(SQLIdentifierExpr(it.second.column))
        }

        return this
    }

    infix fun into(table: String): Insert {
        sqlInsert.tableSource = SQLExprTableSource(table)
        return this
    }

    infix fun columns(columns: List<String>): Insert {
        columns.forEach {
            this.columns.add(it)
            this.sqlInsert.addColumn(SQLIdentifierExpr(it))
        }

        return this
    }

    fun columns(vararg columns: String): Insert {
        return columns(*columns)
    }

    infix fun value(obj: Any): Insert {
        val clazz = obj::class
        val properties = clazz.declaredMemberProperties.map { it.name to it.getter.call(obj) }.toMap()
        val values = columns.map { properties[it] }

        val valuesClause = SQLInsertStatement.ValuesClause()
        values.forEach { valuesClause.addValue(getExpr(it)) }
        sqlInsert.addValueCause(valuesClause)

        return this
    }

    infix fun values(objList: List<Any>): Insert {
        objList.forEach {
            value(it)
        }

        return this
    }

    infix fun nativeValue(value: List<Any>): Insert {
        val valuesClause = SQLInsertStatement.ValuesClause()
        value.forEach { valuesClause.addValue(getExpr(it)) }
        sqlInsert.addValueCause(valuesClause)

        return this
    }

    fun naviteValue(vararg value: Any): Insert {
        return nativeValue(value.toList())
    }

    infix fun nativeValues(values: List<List<Any>>): Insert {
        values.forEach {
            nativeValue(it)
        }

        return this
    }

    fun naviteValues(vararg value: List<Any>): Insert {
        return nativeValues(value.toList())
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
        val result = database.exec(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }
        return result
    }
}