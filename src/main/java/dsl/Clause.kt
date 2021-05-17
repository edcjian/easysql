package dsl

import com.alibaba.druid.sql.SQLUtils
import expr.*

infix fun String.alias(alias: String): Query {
    return QueryColumn(this, alias)
}

infix fun <T> String.then(then: T): CaseBranch<T> {
    val expr = QueryExpr(SQLUtils.toSQLExpr(this))
    return CaseBranch(expr, then)
}

infix fun Query.alias(alias: String): Query {
    this.alias = alias
    return this
}

fun column(column: String): QueryColumn {
    return QueryColumn(column, null)
}

fun column(column: String, alias: String): QueryColumn {
    return QueryColumn(column, alias)
}

fun TableSchema.column(name: String): QueryTableColumn {
    return QueryTableColumn(this.tableName, name)
}

fun QueryTableColumn.inct(): QueryTableColumn {
    this.incr = true
    return this
}

fun <T> const(value: T): QueryConst<T> {
    return QueryConst(value)
}

fun <T> const(value: T, alias: String): QueryConst<T> {
    return QueryConst(value, alias)
}

fun <T> value(value: T): QueryConst<T> {
    return QueryConst(value)
}

fun <T> value(value: T, alias: String): QueryConst<T> {
    return QueryConst(value, alias)
}

infix fun <T> QueryBinary.then(then: T): CaseBranch<T> {
    return CaseBranch(this, then)
}

fun <T> case(vararg conditions: CaseBranch<T>): QueryCase<T> {
    return QueryCase(conditions.toList())
}

fun <T> case(condition: String, then: T): QueryCase<T> {
    val expr = QueryExpr(SQLUtils.toSQLExpr(condition))
    val caseBranch = CaseBranch(expr, then)
    return QueryCase(listOf(caseBranch))
}

infix fun <T> QueryCase<T>.elseIs(value: T?): QueryCase<T> {
    return if (value != null) {
        QueryCase(this.conditions, value)
    } else {
        this
    }
}