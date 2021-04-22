package dsl

import com.alibaba.druid.sql.ast.expr.SQLAggregateOption
import expr.Query
import expr.QueryAggFunction
import expr.QueryColumn
import expr.QueryExprFunction

fun count(): Query {
    return QueryAggFunction("COUNT", listOf())
}

fun count(query: Query): Query {
    return QueryAggFunction("COUNT", listOf(query))
}

fun count(column: String): Query {
    return QueryAggFunction("COUNT", listOf(QueryColumn(column)))
}

fun countDistinct(query: Query): Query {
    return QueryAggFunction("COUNT", listOf(query), SQLAggregateOption.DISTINCT)
}

fun countDistinct(column: String): Query {
    return QueryAggFunction("COUNT", listOf(column(column)), SQLAggregateOption.DISTINCT)
}

fun sum(query: Query): Query {
    return QueryAggFunction("SUM", listOf(query))
}

fun sum(column: String): Query {
    return QueryAggFunction("SUM", listOf(QueryColumn(column)))
}

fun avg(query: Query): Query {
    return QueryAggFunction("AVG", listOf(query))
}

fun avg(column: String): Query {
    return QueryAggFunction("AVG", listOf(QueryColumn(column)))
}

fun max(query: Query): Query {
    return QueryAggFunction("MAX", listOf(query))
}

fun max(column: String): Query {
    return QueryAggFunction("MAX", listOf(QueryColumn(column)))
}

fun min(query: Query): Query {
    return QueryAggFunction("MIN", listOf(query))
}

fun min(column: String): Query {
    return QueryAggFunction("MIN", listOf(QueryColumn(column)))
}

fun concat(vararg query: Query): Query {
    return QueryExprFunction("CONCAT", query.toList())
}

fun concatWs(separator: String, vararg query: Query): Query {
    listOf<Query>(const(separator)) + query.toList()
    return QueryExprFunction("CONCAT_WS", listOf<Query>(const(separator)) + query.toList())
}