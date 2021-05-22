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
import kotlin.reflect.KProperty
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.declaredMembers
import kotlin.reflect.jvm.javaField

class Insert(
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : ReviseQuery() {
    private var sqlInsert = SQLInsertStatement()

    private var columns = mutableListOf<String>()

    private var records = mutableListOf<Any>()

    private var incrKey = ""

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
            .filter {
                if (it.second.incr) {
                    incrKey = it.first
                }
                !it.second.incr
            }

        properties.forEach {
            columns.add(it.first)
            sqlInsert.addColumn(SQLIdentifierExpr(it.second.column))
        }

        return this
    }

    infix fun value(obj: Any): Insert {
        val clazz = obj::class
        val properties = clazz.declaredMemberProperties.map { it.name to it.getter.call(obj) }.toMap()
        val values = columns.map { properties[it] }

        val valuesClause = SQLInsertStatement.ValuesClause()
        values.forEach { valuesClause.addValue(getExpr(it)) }
        sqlInsert.addValueCause(valuesClause)

        records.add(obj)

        return this
    }

    infix fun <T : Any> values(objList: List<T>): Insert {
        objList.forEach {
            value(it)
        }

        return this
    }

    fun <T : Any> values(vararg obj: T): Insert {
        return values(obj.toList())
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
            val clazz = records[index]::class
            val field = (clazz.declaredMembers.find { it.name == incrKey } as KProperty).javaField
            field?.isAccessible = true
            val convert = convertIncrKey(item, field?.type?.typeName!!)
            field.set(records[index], convert)
            field.isAccessible = false
        }
        return this.sqlInsert.valuesList.size
    }

    private fun convertIncrKey(key: Long, javaType: String): Any {
        return when (javaType) {
            "java.lang.Byte" -> key.toByte()
            "java.lang.Short" -> key.toShort()
            "java.lang.Integer" -> key.toInt()
            "java.lang.Long" -> key
            else -> key.toString()
        }
    }
}