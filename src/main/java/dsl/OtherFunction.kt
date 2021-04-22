package dsl

import com.alibaba.druid.sql.ast.SQLOrderingSpecification
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption
import expr.*

fun ifNull(query: Query, value: Query): Query {
    return QueryExprFunction("*IFNULL", listOf(query, value))
}

fun <T> ifNull(query: Query, value: T): Query {
    return ifNull(query, const(value))
}

fun cast(query: Query, type: String): Query {
    return QueryCast(query, type)
}

fun stringAgg(
    query: Query,
    separator: String,
    orderBy: List<AggOrderBy> = listOf(),
    distinct: Boolean = false
): Query {
    val distinctType = if (distinct) {
        SQLAggregateOption.DISTINCT
    } else {
        null
    }
    return QueryAggFunction("*STRING_AGG", listOf(query, const(separator)), option = distinctType, orderBy = orderBy)
}

fun arrayAgg(
    query: Query,
    separator: String,
    orderBy: List<AggOrderBy> = listOf(),
    distinct: Boolean = false
): Query {
    val distinctType = if (distinct) {
        SQLAggregateOption.DISTINCT
    } else {
        null
    }
    return QueryAggFunction("*ARRAY_AGG", listOf(query, const(separator)), option = distinctType, orderBy = orderBy)
}

fun findInSet(value: Query, query: Query): Query {
    return QueryExprFunction("*FIND_IN_SET", listOf(value, query))
}

fun findInSet(value: String, query: Query): Query {
    return findInSet(const(value), query)
}

fun jsonLength(query: Query): Query {
    return  if (query is QueryJson) {
        QueryExprFunction("*JSON_LENGTH", listOf(query.query , query.initQuery, const(query.chain)))
    } else {
        QueryExprFunction("*JSON_LENGTH", listOf(query))
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