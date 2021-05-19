package query.insert

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.QueryTableColumn
import expr.TableSchema
import visitor.getDbType
import visitor.getExpr
import java.sql.Connection
import kotlin.reflect.full.declaredMemberProperties

class Insert(var db: DB = DB.MYSQL, private var conn: Connection? = null, private var isTransaction: Boolean = false) {
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

    infix fun values(obj: Any): Insert {
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
            values(it)
        }

        return this
    }

    fun sql(): String {
        return SQLUtils.toSQLString(
            sqlInsert,
            sqlInsert.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }

    override fun toString(): String {
        return sql()
    }

    fun exec(): Int {
        val result = database.exec(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }
        return result
    }
}