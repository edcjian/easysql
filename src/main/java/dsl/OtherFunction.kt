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
    return QueryExprFunction("*JSON_LENGTH", listOf(query))
}