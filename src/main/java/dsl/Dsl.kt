package dsl

import com.alibaba.druid.DbType
import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.SQLExpr
import com.alibaba.druid.sql.ast.SQLOrderBy
import com.alibaba.druid.sql.ast.SQLOrderingSpecification
import com.alibaba.druid.sql.ast.expr.*
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType
import com.alibaba.druid.sql.ast.statement.SQLSelect
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem
import com.alibaba.druid.sql.ast.statement.SQLUnionOperator
import expr.*
import select.SelectQuery
import select.UnionSelect
import java.math.BigDecimal
import java.util.*

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

fun jsonLength(query: Query, dbType: DB): Query {
    return when (dbType) {
        DB.MYSQL -> {
            if (query is QueryJson) {
                QueryExprFunction("JSON_LENGTH", listOf(query.query, const(query.chain)))
            } else {
                QueryExprFunction("JSON_LENGTH", listOf(query))
            }
        }
        DB.PGSQL -> {
            val arg = if (query is QueryJson) {
                query
            } else {
                cast(query, "JSONB")
            }
            QueryExprFunction("JSONB_ARRAY_LENGTH", listOf(arg))
        }
        // TODO
        else -> throw TypeCastException("暂不支持该数据库使用此函数")
    }
}

// FIXME 此处需要重写 需要添加List<AggOrderBy>.orderBy
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

infix fun Query.eq(query: Query): QueryBinary {
    return QueryBinary(this, "=", query)
}

infix fun <T> Query.eq(query: T): QueryBinary {
    return QueryBinary(this, "=", const(query))
}

infix fun Query.eq(query: SelectQuery): QueryBinary {
    return QueryBinary(this, "=", QuerySub(query))
}

infix fun Query.ne(query: Query): QueryBinary {
    return QueryBinary(this, "<>", query)
}

infix fun <T> Query.ne(query: T): QueryBinary {
    return QueryBinary(this, "<>", const(query))
}

infix fun Query.ne(query: SelectQuery): QueryBinary {
    return QueryBinary(this, "<>", QuerySub(query))
}

infix fun Query.gt(query: Query): QueryBinary {
    return QueryBinary(this, ">", query)
}

infix fun <T> Query.gt(query: T): QueryBinary {
    return QueryBinary(this, ">", const(query))
}

infix fun Query.gt(query: SelectQuery): QueryBinary {
    return QueryBinary(this, ">", QuerySub(query))
}

infix fun Query.ge(query: Query): QueryBinary {
    return QueryBinary(this, ">=", query)
}

infix fun <T> Query.ge(query: T): QueryBinary {
    return QueryBinary(this, ">=", const(query))
}

infix fun Query.ge(query: SelectQuery): QueryBinary {
    return QueryBinary(this, ">=", QuerySub(query))
}

infix fun Query.lt(query: Query): QueryBinary {
    return QueryBinary(this, "<", query)
}

infix fun <T> Query.lt(query: T): QueryBinary {
    return QueryBinary(this, "<", const(query))
}

infix fun Query.lt(query: SelectQuery): QueryBinary {
    return QueryBinary(this, "<", QuerySub(query))
}

infix fun Query.le(query: Query): QueryBinary {
    return QueryBinary(this, "<=", query)
}

infix fun <T> Query.le(query: T): QueryBinary {
    return QueryBinary(this, "<=", const(query))
}

infix fun Query.le(query: SelectQuery): QueryBinary {
    return QueryBinary(this, "<=", QuerySub(query))
}

fun Query.json(value: Any, operator: String = "->"): QueryJson {
    val chain = when (value) {
        is Int -> "[$value]"
        is String -> ".$value"
        else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
    }
    return QueryJson(this, this, operator, value, "$$chain")
}

fun Query.jsonText(value: Any): QueryJson {
    return json(value, "->>")
}

fun QueryJson.json(value: Any, operator: String = "->"): QueryJson {
    val chain = when (value) {
        is Int -> "[$value]"
        is String -> ".$value"
        else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
    }
    return QueryJson(this, this.initQuery, operator, value, "${this.chain}$chain")
}

fun QueryJson.jsonText(value: Any): QueryJson {
    return json(value, "->>")
}

fun Query.isNull(): QueryBinary {
    return QueryBinary(this, "IS", null)
}

fun Query.isNotNull(): QueryBinary {
    return QueryBinary(this, "IS NOT", null)
}

infix fun Query.like(query: String): QueryBinary {
    return QueryBinary(this, "LIKE", const(query))
}

infix fun Query.notLike(query: String): QueryBinary {
    return QueryBinary(this, "NOT LIKE", const(query))
}

fun <T> inList(query: Query, list: List<T>, isNot: Boolean = false): Query {
    return QueryInList(query, list, isNot)
}

fun inList(query: Query, subQuery: SelectQuery, isNot: Boolean = false): Query {
    return QueryInSubQuery(query, subQuery, isNot)
}

infix fun <T> Query.inList(query: List<T>): Query {
    return inList(this, query)
}

infix fun Query.inList(query: SelectQuery): Query {
    return inList(this, query)
}

infix fun <T> Query.notInList(query: List<T>): Query {
    return inList(this, query, true)
}

infix fun Query.notInList(query: SelectQuery): Query {
    return inList(this, query, true)
}

infix fun Query.and(query: Query): QueryBinary {
    return QueryBinary(this, "AND", query)
}

infix fun Query.or(query: Query): QueryBinary {
    return QueryBinary(this, "OR", query)
}

infix fun Query.xor(query: Query): QueryBinary {
    return QueryBinary(this, "XOR", query)
}

infix fun SelectQuery.union(select: SelectQuery): SelectQuery {
    return UnionSelect(this, SQLUnionOperator.UNION, select, this.getDbType())
}

infix fun SelectQuery.unionAll(select: SelectQuery): SelectQuery {
    return UnionSelect(this, SQLUnionOperator.UNION_ALL, select, this.getDbType())
}

fun getQueryExpr(query: Query?, dbType: DB): QueryExpr {
    return when (query) {
        null -> QueryExpr(SQLNullExpr())
        is QueryColumn -> {
            if (query.column.contains(".")) {
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
        is QueryExprFunction -> {
            if (query.name == "*IFNULL") {
                return visitFunctionIfNull(query, dbType)
            }

            if (query.name == "*FIND_IN_SET") {
                return visitFunctionFindInSet(query, dbType)
            }

            val expr = SQLMethodInvokeExpr()
            expr.methodName = query.name
            if (query.args.isEmpty()) {
                expr.addArgument(SQLAllColumnExpr())
            } else {
                query.args.map { getQueryExpr(it, dbType).expr }.forEach { expr.addArgument(it) }
            }
            QueryExpr(expr, query.alias)
        }
        is QueryAggFunction -> {
            if (query.name == "*STRING_AGG") {
                return visitFunctionStringAgg(query, dbType)
            }

            if (query.name == "*ARRAY_AGG") {
                return visitFunctionArrayAgg(query, dbType)
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
            QueryExpr(expr, query.alias)
        }
        is QueryConst<*> -> QueryExpr(getExpr(query.value), query.alias)
        is QueryBinary -> {
            val expr = SQLBinaryOpExpr()
            val left = getQueryExpr(query.left, dbType).expr
            val operator = getBinaryOperator(query.operator)
            val right = getQueryExpr(query.right, dbType).expr
            expr.left = left
            expr.operator = operator
            expr.right = right
            QueryExpr(expr, query.alias)
        }
        is QueryExpr -> {
            query
        }
        is QueryCase<*> -> {
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
            QueryExpr(expr, query.alias)
        }
        is QuerySub -> {
            val expr = SQLQueryExpr()
            val sqlSelect = SQLSelect()
            sqlSelect.query = query.selectQuery.getSelect()
            expr.subQuery = sqlSelect
            QueryExpr(expr, query.alias)
        }
        is QueryTableColumn -> {
            val expr = SQLPropertyExpr()
            expr.name = query.column
            expr.owner = SQLIdentifierExpr(query.table)
            QueryExpr(expr, query.alias)
        }
        is QueryJson -> {
            val operator = getBinaryOperator(query.operator)
            when (dbType) {
                DB.PGSQL -> {
                    val valueExpr = when (query.value) {
                        is Int -> SQLNumberExpr(query.value)
                        is String -> SQLCharExpr(query.value)
                        else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
                    }
                    val expr =
                        SQLBinaryOpExpr(getQueryExpr(cast(query.query, "JSONB"), dbType).expr, operator, valueExpr)
                    QueryExpr(expr, query.alias)
                }
                DB.MYSQL -> {
                    val expr =
                        SQLBinaryOpExpr(getQueryExpr(query.initQuery, dbType).expr, operator, SQLCharExpr(query.chain))
                    QueryExpr(expr, query.alias)
                }
                else -> throw TypeCastException("Json操作暂不支持此数据库")
            }
        }
        is QueryCast -> {
            val dataType = SQLCharacterDataType(query.type)
            val expr = SQLCastExpr()
            expr.expr = getQueryExpr(query.query, dbType).expr
            expr.dataType = dataType
            QueryExpr(expr, query.alias)
        }
        is QueryInList<*> -> {
            val expr = SQLInListExpr()
            expr.isNot = query.isNot
            expr.expr = getQueryExpr(query.query, dbType).expr
            query.list.forEach { expr.addTarget(getExpr(it)) }
            QueryExpr(expr)
        }
        is QueryInSubQuery -> {
            val expr = SQLInSubQueryExpr()
            expr.isNot = query.isNot
            expr.expr = getQueryExpr(query.query, dbType).expr
            expr.subQuery = SQLSelect(query.subQuery.getSelect())
            QueryExpr(expr)
        }
        else -> throw TypeCastException("未找到对应的查询类型")
    }
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

fun getBinaryOperator(operator: String): SQLBinaryOperator {
    return when (operator) {
        "IS" -> SQLBinaryOperator.Is
        "IS NOT" -> SQLBinaryOperator.IsNot
        "=" -> SQLBinaryOperator.Equality
        "!=", "<>" -> SQLBinaryOperator.NotEqual
        "LIKE" -> SQLBinaryOperator.Like
        "NOT LIKE" -> SQLBinaryOperator.NotLike
        ">" -> SQLBinaryOperator.GreaterThan
        ">=" -> SQLBinaryOperator.GreaterThanOrEqual
        "<" -> SQLBinaryOperator.LessThan
        "<=" -> SQLBinaryOperator.LessThanOrEqual
        "AND" -> SQLBinaryOperator.BooleanAnd
        "OR" -> SQLBinaryOperator.BooleanOr
        "XOR" -> SQLBinaryOperator.BooleanXor
        "+" -> SQLBinaryOperator.Add
        "-" -> SQLBinaryOperator.Subtract
        "*" -> SQLBinaryOperator.Multiply
        "/" -> SQLBinaryOperator.Divide
        "%" -> SQLBinaryOperator.Modulus
        "->" -> SQLBinaryOperator.SubGt
        "->>" -> SQLBinaryOperator.SubGtGt
        else -> throw TypeCastException("未找到对应的二元操作符")
    }
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