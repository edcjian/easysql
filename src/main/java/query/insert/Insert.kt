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
import kotlin.reflect.full.declaredMemberProperties

class Insert(db: DB = DB.MYSQL) {
    private var sqlInsert = SQLInsertStatement()

    private var dbType: DB = db

    private lateinit var columns: List<String>

    init {
        sqlInsert.dbType = getDbType(dbType)
    }

    infix fun <T : TableSchema> into(table: T): Insert {
        sqlInsert.tableSource = SQLExprTableSource(table.tableName)

        val clazz = table::class
        val declaredMemberProperties = clazz.declaredMemberProperties
        columns = declaredMemberProperties.map { it.name }
        val properties = declaredMemberProperties.map { it.getter.call(table) }
        properties.filterIsInstance<QueryTableColumn>().map { SQLIdentifierExpr(it.column) }.forEach {
            sqlInsert.addColumn(it)
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
}