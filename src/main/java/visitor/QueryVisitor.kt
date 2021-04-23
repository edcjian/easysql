package visitor

import com.alibaba.druid.DbType
import com.alibaba.druid.sql.ast.SQLExpr
import com.alibaba.druid.sql.ast.SQLOrderBy
import com.alibaba.druid.sql.ast.expr.*
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType
import com.alibaba.druid.sql.ast.statement.SQLSelect
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem
import dsl.cast
import dsl.const
import dsl.getBinaryOperator
import dsl.isNull
import expr.*
import java.math.BigDecimal
import java.util.*

fun getQueryExpr(query: Query?, dbType: DB): QueryExpr {
    return when (query) {
        null -> QueryExpr(SQLNullExpr())

        is QueryColumn -> visitQueryColumn(query, dbType)

        is QueryExprFunction -> visitQueryExprFunction(query, dbType)

        is QueryAggFunction -> visitQueryAggFunction(query, dbType)

        is QueryConst<*> -> QueryExpr(getExpr(query.value), query.alias)

        is QueryBinary -> visitQueryBinary(query, dbType)

        is QueryExpr -> query

        is QueryCase<*> -> visitQueryCase(query, dbType)

        is QuerySub -> visitQuerySub(query, dbType)

        is QueryTableColumn -> visitQueryTableColumn(query, dbType)

        is QueryJson -> visitQueryJson(query, dbType)

        is QueryCast -> visitQueryCast(query, dbType)

        is QueryInList<*> -> visitQueryInList(query, dbType)

        is QueryInSubQuery -> visitQueryInSubQuery(query, dbType)

        is QueryBetween<*> -> visitQueryBetween(query, dbType)

        else -> throw TypeCastException("未找到对应的查询类型")
    }
}

fun visitQueryColumn(query: QueryColumn, dbType: DB): QueryExpr {
    return if (query.column.contains(".")) {
        val expr = SQLPropertyExpr()
        val split = query.column.split(".")
        expr.name = split.last()
        expr.owner = SQLIdentifierExpr(split.first())
        QueryExpr(expr, query.alias)
    } else {
        val expr = SQLIdentifierExpr()
        expr.name = query.column
        QueryExpr(expr, query.alias)
    }
}

val specialExprFunction = mapOf("*IFNULL" to ::visitFunctionIfNull,
        "*FIND_IN_SET" to ::visitFunctionFindInSet,
        "*JSON_LENGTH" to ::visitFunctionJsonLength)

fun visitQueryExprFunction(query: QueryExprFunction, dbType: DB): QueryExpr {
    if (specialExprFunction.contains(query.name)) {
        return specialExprFunction[query.name]!!.let { it(query, dbType) }
    }

    val expr = SQLMethodInvokeExpr()
    expr.methodName = query.name
    if (query.args.isEmpty()) {
        expr.addArgument(SQLAllColumnExpr())
    } else {
        query.args.map { getQueryExpr(it, dbType).expr }.forEach { expr.addArgument(it) }
    }
    return QueryExpr(expr, query.alias)
}

val specialAggFunction = mapOf("*STRING_AGG" to ::visitFunctionStringAgg, "*ARRAY_AGG" to ::visitFunctionArrayAgg)

fun visitQueryAggFunction(query: QueryAggFunction, dbType: DB): QueryExpr {
    if (specialAggFunction.contains(query.name)) {
        return specialAggFunction[query.name]!!.let { it(query, dbType) }
    }

    val expr = SQLAggregateExpr(query.name)
    expr.option = query.option
    if (query.args.isEmpty()) {
        expr.addArgument(SQLAllColumnExpr())
    } else {
        query.args.map { getQueryExpr(it, dbType).expr }.forEach { expr.addArgument(it) }
    }
    query.attributes?.let { attributes ->
        attributes.forEach { (k, v) -> expr.putAttribute(k, getQueryExpr(v, dbType).expr) }
    }
    if (query.orderBy.isNotEmpty()) {
        val orderBy = SQLOrderBy()
        query.orderBy.forEach {
            val orderByItem = SQLSelectOrderByItem()
            orderByItem.expr = getQueryExpr(it.query, dbType).expr
            orderByItem.type = it.order
            orderBy.addItem(orderByItem)
        }
        expr.orderBy = orderBy
    }
    return QueryExpr(expr, query.alias)
}

fun visitQueryBinary(query: QueryBinary, dbType: DB): QueryExpr {
    val expr = SQLBinaryOpExpr()
    val left = getQueryExpr(query.left, dbType).expr
    val operator = getBinaryOperator(query.operator)
    val right = getQueryExpr(query.right, dbType).expr
    expr.left = left
    expr.operator = operator
    expr.right = right
    return QueryExpr(expr, query.alias)
}

fun visitQueryCase(query: QueryCase<*>, dbType: DB): QueryExpr {
    val expr = SQLCaseExpr()
    query.conditions.forEach {
        val then = if (it.then is Query) {
            getQueryExpr(it.then, dbType).expr
        } else {
            getExpr(it.then)
        }
        expr.addItem(getQueryExpr(it.query, dbType).expr, then)
    }

    val default = if (query.default is Query) {
        getQueryExpr(query.default, dbType).expr
    } else {
        getExpr(query.default)
    }
    expr.elseExpr = default
    return QueryExpr(expr, query.alias)
}

fun visitQuerySub(query: QuerySub, dbType: DB): QueryExpr {
    val expr = SQLQueryExpr()
    val sqlSelect = SQLSelect()
    sqlSelect.query = query.selectQuery.getSelect()
    expr.subQuery = sqlSelect
    return QueryExpr(expr, query.alias)
}

fun visitQueryTableColumn(query: QueryTableColumn, dbType: DB): QueryExpr {
    val expr = SQLPropertyExpr()
    expr.name = query.column
    expr.owner = SQLIdentifierExpr(query.table)
    return QueryExpr(expr, query.alias)
}

fun visitQueryJson(query: QueryJson, dbType: DB): QueryExpr {
    val operator = getBinaryOperator(query.operator)
    return when (dbType) {
        DB.PGSQL -> {
            val valueExpr = when (query.value) {
                is Int -> SQLNumberExpr(query.value)
                is String -> SQLCharExpr(query.value)
                else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
            }

            val cast = if (query.query is QueryJson) {
                query.query
            } else {
                cast(query.query, "JSONB")
            }

            val expr = SQLBinaryOpExpr(getQueryExpr(cast, dbType).expr, operator, valueExpr)
            QueryExpr(expr, query.alias)
        }
        DB.MYSQL -> {
            val visitMysql = visitMysqlQueryJson(query)
            val expr = SQLBinaryOpExpr(getQueryExpr(visitMysql.second, dbType).expr, operator, SQLCharExpr(visitMysql.first))
            QueryExpr(expr, query.alias)
        }
        else -> throw TypeCastException("Json操作暂不支持此数据库")
    }
}

fun visitQueryCast(query: QueryCast, dbType: DB): QueryExpr {
    val dataType = SQLCharacterDataType(query.type)
    val expr = SQLCastExpr()
    expr.expr = getQueryExpr(query.query, dbType).expr
    expr.dataType = dataType
    return QueryExpr(expr, query.alias)
}

fun visitQueryInList(query: QueryInList<*>, dbType: DB): QueryExpr {
    val expr = SQLInListExpr()
    expr.isNot = query.isNot
    expr.expr = getQueryExpr(query.query, dbType).expr
    query.list.forEach { expr.addTarget(getExpr(it)) }
    return QueryExpr(expr)
}

fun visitQueryInSubQuery(query: QueryInSubQuery, dbType: DB): QueryExpr {
    val expr = SQLInSubQueryExpr()
    expr.isNot = query.isNot
    expr.expr = getQueryExpr(query.query, dbType).expr
    expr.subQuery = SQLSelect(query.subQuery.getSelect())
    return QueryExpr(expr)
}

fun visitQueryBetween(query: QueryBetween<*>, dbType: DB): QueryExpr {
    val expr = SQLBetweenExpr()
    expr.testExpr = getQueryExpr(query.query, dbType).expr
    expr.beginExpr = getExpr(query.start)
    expr.endExpr = getExpr(query.end)
    expr.isNot = query.isNot
    return QueryExpr(expr)
}

fun visitFunctionIfNull(query: QueryExprFunction, dbType: DB): QueryExpr {
    val function = when (dbType) {
        DB.MYSQL -> QueryExprFunction("IFNULL", listOf(query.args[0], query.args[1]))
        DB.PGSQL -> QueryExprFunction("COALESCE", listOf(query.args[0], query.args[1]))
        DB.ORACLE -> QueryExprFunction("NVL", listOf(query.args[0], query.args[1]))
        DB.HIVE -> QueryExprFunction("IF", listOf(query.args[0].isNull(), query.args[1], query.args[0]))
    }
    return QueryExpr(getQueryExpr(function, dbType).expr, query.alias)
}

fun visitFunctionFindInSet(query: QueryExprFunction, dbType: DB): QueryExpr {
    val function = when (dbType) {
        DB.MYSQL -> QueryExprFunction("FIND_IN_SET", listOf(query.args[0], query.args[1]))
        DB.PGSQL -> QueryBinary(
                cast(query.args[0], "VARCHAR"),
                "=",
                QueryExprFunction("ANY", listOf(QueryExprFunction("STRING_TO_ARRAY", listOf(query.args[1], const(",")))))
        )
        // TODO
        else -> throw TypeCastException("暂不支持该数据库使用此函数")
    }

    return QueryExpr(getQueryExpr(function, dbType).expr, query.alias)
}

fun visitFunctionJsonLength(query: QueryExprFunction, dbType: DB): QueryExpr {
    val arg0 = query.args[0]

    val function = when (dbType) {
        DB.MYSQL -> {
            if (arg0 is QueryJson) {
                val visitMysql = visitMysqlQueryJson(arg0)
                QueryExprFunction("JSON_LENGTH", listOf(visitMysql.second, const(visitMysql.first)))
            } else {
                QueryExprFunction("JSON_LENGTH", listOf(arg0))
            }
        }
        DB.PGSQL -> {
            if (arg0 is QueryJson) {
                QueryExprFunction("JSONB_ARRAY_LENGTH", listOf(arg0))
            } else {
                QueryExprFunction("JSONB_ARRAY_LENGTH", listOf(cast(arg0, "JSONB")))
            }
        }
        // TODO
        else -> throw TypeCastException("暂不支持该数据库使用此函数")
    }

    return QueryExpr(getQueryExpr(function, dbType).expr, query.alias)
}

fun visitFunctionStringAgg(query: QueryAggFunction, dbType: DB): QueryExpr {
    val function = when (dbType) {
        DB.MYSQL -> QueryAggFunction(
                "GROUP_CONCAT",
                listOf(query.args[0]),
                attributes = mapOf("SEPARATOR" to query.args[1]),
                option = query.option,
                orderBy = query.orderBy
        )
        DB.PGSQL -> QueryAggFunction(
                "STRING_AGG",
                listOf(cast(query.args[0], "VARCHAR"), query.args[1]),
                option = query.option,
                orderBy = query.orderBy
        )
        // TODO
        else -> throw TypeCastException("暂不支持该数据库使用此函数")
    }
    return QueryExpr(getQueryExpr(function, dbType).expr, query.alias)
}


fun visitFunctionArrayAgg(query: QueryAggFunction, dbType: DB): QueryExpr {
    val function = when (dbType) {
        DB.MYSQL -> QueryAggFunction(
                "GROUP_CONCAT",
                listOf(query.args[0]),
                attributes = mapOf("SEPARATOR" to query.args[1]),
                option = query.option,
                orderBy = query.orderBy
        )
        DB.PGSQL -> QueryExprFunction(
                "ARRAY_TO_STRING",
                listOf(
                        QueryAggFunction(
                                "ARRAY_AGG",
                                listOf(cast(query.args[0], "VARCHAR")),
                                option = query.option,
                                orderBy = query.orderBy
                        ), query.args[1]
                )
        )
        // TODO
        else -> throw TypeCastException("暂不支持该数据库使用此函数")
    }
    return QueryExpr(getQueryExpr(function, dbType).expr, query.alias)
}

fun visitMysqlQueryJson(queryJson: QueryJson): Pair<String, Query> {
    fun transMysqlJson(value: Any): String {
        return when (value) {
            is Number -> "[$value]"
            is String -> ".$value"
            else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
        }
    }

    fun visit(queryJson: QueryJson): Pair<String, Query> {
        val query = queryJson.query

        if (query is QueryJson) {
            val result = visit(query)
            return Pair(result.first + transMysqlJson(query.value), result.second)
        }
        return Pair("", query)
    }

    val visitResult = visit(queryJson)
    return Pair("$" + visitResult.first + transMysqlJson(queryJson.value), visitResult.second)
}

fun <T> getExpr(value: T): SQLExpr {
    return when (value) {
        null -> SQLNullExpr()
        is Number -> {
            val expr = SQLNumberExpr()
            expr.number = value
            expr
        }
        is Date -> {
            SQLDateExpr(value)
        }
        is BigDecimal -> {
            SQLDecimalExpr(value)
        }
        is String -> {
            val expr = SQLCharExpr()
            expr.text = value
            expr
        }
        is Boolean -> {
            SQLBooleanExpr(value)
        }
        is List<*> -> {
            val expr = SQLListExpr()
            value.forEach { expr.addItem(getExpr(it)) }
            expr
        }
        else -> throw TypeCastException("未找到对应的数据类型")
    }
}

fun getDbType(dbType: DB): DbType {
    return when (dbType) {
        DB.MYSQL -> DbType.mysql
        DB.ORACLE -> DbType.oracle
        DB.PGSQL -> DbType.postgresql
        DB.HIVE -> DbType.hive
    }
}