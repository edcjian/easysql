package dsl

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.SQLOrderingSpecification
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

fun allColumn(): QueryAllColumn {
    return QueryAllColumn(null)
}

fun QueryTableColumn.incr(): QueryTableColumn {
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

fun orderBy(order: SQLOrderingSpecification = SQLOrderingSpecification.ASC, vararg query: Query): List<AggOrderBy> {
    return query.map { AggOrderBy(it, order) }
}

fun orderByAsc(vararg query: Query): List<AggOrderBy> {
    return orderBy(SQLOrderingSpecification.ASC, *query)
}

fun orderByDesc(vararg query: Query): List<AggOrderBy> {
    return orderBy(SQLOrderingSpecification.DESC, *query)
}

fun List<AggOrderBy>.orderBy(
    order: SQLOrderingSpecification = SQLOrderingSpecification.ASC,
    vararg query: Query
): List<AggOrderBy> {
    val list = query.map { AggOrderBy(it, order) }
    return this + list
}

fun List<AggOrderBy>.orderByAsc(
    vararg query: Query
): List<AggOrderBy> {
    return this.orderBy(SQLOrderingSpecification.ASC, *query)
}

fun List<AggOrderBy>.orderByDesc(
    vararg query: Query
): List<AggOrderBy> {
    return this.orderBy(SQLOrderingSpecification.DESC, *query)
}

fun QueryAggFunction.over(partitionBy: List<Query> = listOf(), orderBy: List<AggOrderBy> = listOf()): QueryOver {
    return QueryOver(this, partitionBy, orderBy)
}

fun QueryOver.partitionBy(vararg query: Query): QueryOver {
    return QueryOver(this.function, this.partitionBy + query, this.orderBy)
}

infix fun QueryOver.partitionBy(query: Query): QueryOver {
    return QueryOver(this.function, this.partitionBy + query, this.orderBy)
}

infix fun QueryOver.partitionBy(query: List<Query>): QueryOver {
    return QueryOver(this.function, this.partitionBy + query, this.orderBy)
}

fun QueryOver.orderByAsc(vararg order: Query): QueryOver {
    val orderBy = order.map { AggOrderBy(it, SQLOrderingSpecification.ASC) }
    return QueryOver(this.function, this.partitionBy, this.orderBy + orderBy)
}

fun QueryOver.orderByDesc(vararg order: Query): QueryOver {
    val orderBy = order.map { AggOrderBy(it, SQLOrderingSpecification.DESC) }
    return QueryOver(this.function, this.partitionBy, this.orderBy + orderBy)
}

infix fun QueryOver.orderByAsc(order: Query): QueryOver {
    return QueryOver(this.function, this.partitionBy, this.orderBy + AggOrderBy(order, SQLOrderingSpecification.ASC))
}

infix fun QueryOver.orderByDesc(order: Query): QueryOver {
    return QueryOver(this.function, this.partitionBy, this.orderBy + AggOrderBy(order, SQLOrderingSpecification.DESC))
}

infix fun QueryOver.orderByAsc(order: List<Query>): QueryOver {
    val orderBy = order.map { AggOrderBy(it, SQLOrderingSpecification.ASC) }
    return QueryOver(this.function, this.partitionBy, this.orderBy + orderBy)
}

infix fun QueryOver.orderByDesc(order: List<Query>): QueryOver {
    val orderBy = order.map { AggOrderBy(it, SQLOrderingSpecification.DESC) }
    return QueryOver(this.function, this.partitionBy, this.orderBy + orderBy)
}