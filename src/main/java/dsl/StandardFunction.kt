package dsl

import com.alibaba.druid.sql.ast.expr.SQLAggregateOption
import expr.*

fun count(): QueryAggFunction {
    return QueryAggFunction("COUNT", listOf())
}

fun count(query: Query): QueryAggFunction {
    return QueryAggFunction("COUNT", listOf(query))
}

fun count(column: String): QueryAggFunction {
    return QueryAggFunction("COUNT", listOf(QueryColumn(column)))
}

fun countDistinct(query: Query): QueryAggFunction {
    return QueryAggFunction("COUNT", listOf(query), SQLAggregateOption.DISTINCT)
}

fun countDistinct(column: String): QueryAggFunction {
    return QueryAggFunction("COUNT", listOf(column(column)), SQLAggregateOption.DISTINCT)
}

fun sum(query: Query): QueryAggFunction {
    return QueryAggFunction("SUM", listOf(query))
}

fun sum(column: String): QueryAggFunction {
    return QueryAggFunction("SUM", listOf(QueryColumn(column)))
}

fun avg(query: Query): QueryAggFunction {
    return QueryAggFunction("AVG", listOf(query))
}

fun avg(column: String): QueryAggFunction {
    return QueryAggFunction("AVG", listOf(QueryColumn(column)))
}

fun max(query: Query): QueryAggFunction {
    return QueryAggFunction("MAX", listOf(query))
}

fun max(column: String): QueryAggFunction {
    return QueryAggFunction("MAX", listOf(QueryColumn(column)))
}

fun min(query: Query): QueryAggFunction {
    return QueryAggFunction("MIN", listOf(query))
}

fun min(column: String): QueryAggFunction {
    return QueryAggFunction("MIN", listOf(QueryColumn(column)))
}

fun rank(): QueryAggFunction {
    return QueryAggFunction("RANK", listOf())
}

fun denseRank(): QueryAggFunction {
    return QueryAggFunction("DENSE_RANK", listOf())
}

fun rowNumber(): QueryAggFunction {
    return QueryAggFunction("ROW_NUMBER", listOf())
}

fun concat(vararg query: Query): Query {
    return QueryExprFunction("CONCAT", query.toList())
}

fun concatWs(separator: String, vararg query: Query): Query {
    listOf<Query>(const(separator)) + query.toList()
    return QueryExprFunction("CONCAT_WS", listOf<Query>(const(separator)) + query.toList())
}