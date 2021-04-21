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
    return CaseBranch(getQueryExpr(this), then)
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

fun ifNull(query: Query, value: Query, dbType: DB): Query {
    return when (dbType) {
        DB.MYSQL -> QueryExprFunction("IFNULL", listOf(query, value))
        DB.PGSQL -> QueryExprFunction("COALESCE", listOf(query, value))
        DB.ORACLE -> QueryExprFunction("NVL", listOf(query, value))
        DB.HIVE -> QueryExprFunction("IF", listOf(query.isNull(), value, query))
    }
}

fun cast(query: Query, type: String): Query {
    val dataType = SQLCharacterDataType(type)
    val expr = SQLCastExpr()
    expr.expr = getQueryExpr(query).expr
    expr.dataType = dataType
    return QueryExpr(expr)
}

fun stringAgg(
        query: Query,
        separator: String,
        dbType: DB,
        orderBy: SQLOrderBy? = null,
        distinct: Boolean = false
): Query {
    val distinctType = if (distinct) {
        SQLAggregateOption.DISTINCT
    } else {
        null
    }
    return when (dbType) {
        DB.MYSQL -> QueryAggFunction(
                "GROUP_CONCAT",
                listOf(query),
                attributes = mapOf("SEPARATOR" to const(separator)),
                option = distinctType,
                orderBy = orderBy
        )
        DB.PGSQL -> QueryAggFunction(
                "STRING_AGG",
                listOf(cast(query, "VARCHAR"), const(separator)),
                option = distinctType,
                orderBy = orderBy
        )
        // TODO
        else -> throw TypeCastException("暂不支持该数据库使用此函数")
    }
}

fun arrayAgg(
        query: Query,
        separator: String,
        dbType: DB,
        orderBy: SQLOrderBy? = null,
        distinct: Boolean = false
): Query {
    val distinctType = if (distinct) {
        SQLAggregateOption.DISTINCT
    } else {
        null
    }
    return when (dbType) {
        DB.MYSQL -> QueryAggFunction(
                "GROUP_CONCAT",
                listOf(query),
                attributes = mapOf("SEPARATOR" to const(separator)),
                option = distinctType,
                orderBy = orderBy
        )
        DB.PGSQL -> QueryExprFunction(
                "ARRAY_TO_STRING",
                listOf(
                        QueryAggFunction(
                                "ARRAY_AGG",
                                listOf(cast(query, "VARCHAR")),
                                option = distinctType,
                                orderBy = orderBy
                        ), const(separator)
                )
        )
        // TODO
        else -> throw TypeCastException("暂不支持该数据库使用此函数")
    }
}

fun findInSet(value: Query, query: Query, dbType: DB): Query {
    return when (dbType) {
        DB.MYSQL -> QueryExprFunction("FIND_IN_SET", listOf(value, query))
        DB.PGSQL -> QueryBinary(
                cast(value, "VARCHAR"),
                "=",
                QueryExprFunction("ANY", listOf(QueryExprFunction("STRING_TO_ARRAY", listOf(query, const(",")))))
        )
        // TODO
        else -> throw TypeCastException("暂不支持该数据库使用此函数")
    }
}

fun findInSet(value: String, query: Query, dbType: DB): Query {
    return findInSet(const(value), query, dbType)
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

fun Query.orderBy(order: SQLOrderingSpecification = SQLOrderingSpecification.ASC): SQLOrderBy {
    val orderBy = SQLOrderBy()
    val expr = SQLSelectOrderByItem()
    expr.expr = getQueryExpr(this).expr
    expr.type = order
    orderBy.addItem(expr)
    return orderBy
}

fun Query.orderByAsc(): SQLOrderBy {
    return orderBy()
}

fun Query.orderByDesc(): SQLOrderBy {
    return orderBy(SQLOrderingSpecification.DESC)
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

fun Query.json(value: Any, dbType: DB, operator: String = "->"): QueryJson {
    val query = if (dbType == DB.PGSQL) {
        cast(this, "JSONB")
    } else {
        this
    }

    if (dbType == DB.MYSQL) {
        val chain = when (value) {
            is Int -> "[$value]"
            is String -> ".$value"
            else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
        }
        return QueryJson(query, operator, value, dbType, "$$chain")
    }

    return QueryJson(query, "->", value, dbType)
}

fun Query.jsonText(value: Any, dbType: DB): QueryJson {
    return json(value, dbType, "->>")
}

fun QueryJson.json(value: Any, operator: String = "->"): QueryJson {
    if (!this.chain.isNullOrEmpty()) {
        if (this.db == DB.MYSQL) {
            val chain = when (value) {
                is Int -> "[$value]"
                is String -> ".$value"
                else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
            }
            return QueryJson(this.query, operator, value, this.db, "${this.chain}$chain")
        }
    }
    return QueryJson(this, operator, value, this.db)
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
    val expr = SQLInListExpr()
    expr.isNot = isNot
    expr.expr = getQueryExpr(query).expr
    list.forEach { expr.addTarget(getExpr(it)) }
    return QueryExpr(expr)
}

fun inList(query: Query, subQuery: SelectQuery, isNot: Boolean = false): Query {
    val expr = SQLInSubQueryExpr()
    expr.isNot = isNot
    expr.expr = getQueryExpr(query).expr
    expr.subQuery = SQLSelect(subQuery.getSelect())
    return QueryExpr(expr)
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

fun getQueryExpr(query: Query?): QueryExpr {
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
            val expr = SQLMethodInvokeExpr()
            expr.methodName = query.name
            if (query.args.isEmpty()) {
                expr.addArgument(SQLAllColumnExpr())
            } else {
                query.args.map { getQueryExpr(it).expr }.forEach { expr.addArgument(it) }
            }
            QueryExpr(expr, query.alias)
        }
        is QueryAggFunction -> {
            val expr = SQLAggregateExpr(query.name)
            expr.option = query.option
            if (query.args.isEmpty()) {
                expr.addArgument(SQLAllColumnExpr())
            } else {
                query.args.map { getQueryExpr(it).expr }.forEach { expr.addArgument(it) }
            }
            query.attributes?.let { attributes ->
                attributes.forEach { (k, v) -> expr.putAttribute(k, getQueryExpr(v).expr) }
            }
            query.orderBy?.let { expr.orderBy = it }
            QueryExpr(expr, query.alias)
        }
        is QueryConst<*> -> QueryExpr(getExpr(query.value), query.alias)
        is QueryBinary -> {
            val expr = SQLBinaryOpExpr()
            val left = getQueryExpr(query.left).expr
            val operator = getBinaryOperator(query.operator)
            val right = getQueryExpr(query.right).expr
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
                expr.addItem(it.query.expr, getExpr(it.then))
            }
            expr.elseExpr = getExpr(query.default)
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
            when (query.db) {
                DB.PGSQL -> {
                    val valueExpr = when (query.value) {
                        is Int -> SQLNumberExpr(query.value)
                        is String -> SQLCharExpr(query.value)
                        else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
                    }
                    val expr = SQLBinaryOpExpr(getQueryExpr(query.query).expr, operator, valueExpr)
                    QueryExpr(expr, query.alias)
                }
                DB.MYSQL -> {
                    val expr = SQLBinaryOpExpr(getQueryExpr(query.query).expr, operator, SQLCharExpr(query.chain))
                    QueryExpr(expr, query.alias)
                }
                else -> throw TypeCastException("Json操作暂不支持此数据库")
            }
        }
        else -> throw TypeCastException("未找到对应的查询类型")
    }
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
        is Query -> {
            getQueryExpr(value).expr
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