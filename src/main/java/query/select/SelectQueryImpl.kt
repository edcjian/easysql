package query.select

import expr.QueryTableColumn
import kotlin.reflect.KProperty
import kotlin.reflect.full.companionObjectInstance
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.declaredMembers
import kotlin.reflect.jvm.javaField

abstract class SelectQueryImpl : SelectQuery {
    fun queryMap(): List<Map<String, Any>> {
        val result = database.query(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }
        return result
    }

    fun <T : Any> query(clazz: Class<T>): List<T> {
        val list = database.query(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }
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
        val list = database.query(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }
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

    open fun fetchCount(): Long {
        val result = database.queryCount(conn!!, this.sql()).toLong()
        if (!isTransaction) {
            conn!!.close()
        }
        return result
    }

    open fun exist(): Boolean {
        return fetchCount() > 0
    }

    override fun toString(): String {
        return sql()
    }
}