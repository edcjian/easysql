package query.select

import expr.QueryTableColumn
import java.lang.Exception
import kotlin.reflect.KProperty
import kotlin.reflect.full.companionObjectInstance
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.declaredMembers
import kotlin.reflect.jvm.javaField

abstract class SelectQueryImpl : SelectQuery {
    fun queryMap(): List<Map<String, Any>> {
        val conn = this.dataSource!!.getDataSource().connection
        return database.query(conn, this.sql())
    }

    fun <T : Any> query(clazz: Class<T>): List<T> {
        val conn = this.dataSource!!.getDataSource().connection
        val list = database.query(conn, this.sql())
        val companion = clazz.kotlin.companionObjectInstance ?: throw Exception("实体类需要添加伴生对象")
        val companionClass = companion::class
        val columns = companionClass.declaredMemberProperties
            .map { it.getter.call(companion) to it.name }
            .filter { it.first is QueryTableColumn }
            .map { (it.first as QueryTableColumn).column to it.second }
            .toMap()

        return list.map {
            val rowClass = clazz
            val row = rowClass.newInstance()

            columns.forEach { column ->
                val fieldName = column.value
                val field = (rowClass.kotlin.declaredMembers.find { it.name == fieldName } as KProperty).javaField
                field?.isAccessible = true
                field?.set(row, it[column.key])
            }

            row
        }
    }

    inline fun <reified T> query(): List<T> {
        val conn = this.dataSource!!.getDataSource().connection
        val list = database.query(conn, this.sql())
        val companion = T::class.companionObjectInstance ?: throw Exception("实体类需要添加伴生对象")
        val companionClass = companion::class
        val columns = companionClass.declaredMemberProperties
            .map { it.getter.call(companion) to it.name }
            .filter { it.first is QueryTableColumn }
            .map { (it.first as QueryTableColumn).column to it.second }
            .toMap()

        return list.map {
            val rowClass = T::class
            val row = rowClass.java.newInstance()

            columns.forEach { column ->
                val fieldName = column.value
                val field = (rowClass.declaredMembers.find { it.name == fieldName } as KProperty).javaField
                field?.isAccessible = true
                field?.set(row, it[column.key])
            }

            row
        }
    }

    fun count(): Int {
        val conn = this.dataSource!!.getDataSource().connection
        return database.queryCount(conn, this.sql())
    }

    fun exist(): Boolean {
        return count() > 0
    }

    override fun toString(): String {
        return sql()
    }
}