package dsl

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator
import expr.*
import select.SelectQuery

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

infix fun <T> Query.between(value: Pair<T, T>): Query {
    return QueryBetween(this, value.first, value.second, true)
}

fun <T> Query.between(start: T, end: T): Query {
    return QueryBetween(this, start, end)
}

fun <T> Query.notBetween(start: T, end: T): Query {
    return QueryBetween(this, start, end)
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