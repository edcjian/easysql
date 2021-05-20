package expr

import com.alibaba.druid.sql.ast.SQLExpr
import com.alibaba.druid.sql.ast.SQLOrderBy
import com.alibaba.druid.sql.ast.SQLOrderingSpecification
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption
import dsl.const
import query.select.SelectQuery

sealed class Query(open var alias: String?) {
    constructor() : this(null)

    operator fun plus(query: Query): QueryBinary {
        return QueryBinary(this, "+", query)
    }

    operator fun <T> plus(value: T): QueryBinary {
        return QueryBinary(this, "+", const(value))
    }

    operator fun minus(query: Query): QueryBinary {
        return QueryBinary(this, "-", query)
    }

    operator fun <T> minus(value: T): QueryBinary {
        return QueryBinary(this, "-", const(value))
    }

    operator fun times(query: Query): QueryBinary {
        return QueryBinary(this, "*", query)
    }

    operator fun <T> times(value: T): QueryBinary {
        return QueryBinary(this, "*", const(value))
    }

    operator fun div(query: Query): QueryBinary {
        return QueryBinary(this, "/", query)
    }

    operator fun <T> div(value: T): QueryBinary {
        return QueryBinary(this, "/", const(value))
    }

    operator fun rem(query: Query): QueryBinary {
        return QueryBinary(this, "%", query)
    }

    operator fun <T> rem(value: T): QueryBinary {
        return QueryBinary(this, "%", const(value))
    }
}

data class QueryExpr(val expr: SQLExpr, override var alias: String? = null) : Query()

data class QueryColumn(val column: String, override var alias: String? = null) : Query()

data class QueryExprFunction(val name: String, val args: List<Query>, override var alias: String? = null) : Query()

data class QueryAggFunction(
    val name: String,
    val args: List<Query>,
    val option: SQLAggregateOption? = null,
    val attributes: Map<String, Query>? = null,
    val orderBy: List<AggOrderBy> = listOf(),
    override var alias: String? = null
) : Query()

data class QueryConst<T>(val value: T, override var alias: String? = null) : Query()

data class QueryBinary(val left: Query, val operator: String, val right: Query?, override var alias: String? = null) :
    Query()

data class QuerySub(val selectQuery: SelectQuery, override var alias: String? = null) : Query()

data class QueryCase<T>(
    val conditions: List<CaseBranch<T>>,
    val default: T? = null,
    override var alias: String? = null
) : Query()

data class QueryTableColumn(
    val table: String,
    val column: String,
    var incr: Boolean = false,
    override var alias: String? = null
) : Query()

data class QueryJson(
    val query: Query,
    val operator: String,
    val value: Any,
    override var alias: String? = null
) : Query()

data class QueryCast(val query: Query, val type: String, override var alias: String? = null) : Query()

data class QueryInList<T>(
    val query: Query,
    val list: List<T>,
    val isNot: Boolean = false,
    override var alias: String? = null
) : Query()

data class QueryInSubQuery(
    val query: Query,
    val subQuery: SelectQuery,
    val isNot: Boolean = false,
    override var alias: String? = null
) : Query()

data class QueryBetween<T>(
    val query: Query,
    val start: T,
    val end: T,
    val isNot: Boolean = false,
    override var alias: String? = null
) : Query()

data class QueryAllColumn(val owner: String? = null, override var alias: String? = null) : Query()

data class QueryOver(
    val function: QueryAggFunction,
    val partitionBy: List<Query> = listOf(),
    val orderBy: List<AggOrderBy> = listOf(),
    override var alias: String? = null
) : Query()

data class CaseBranch<T>(val query: Query, val then: T)

data class AggOrderBy(val query: Query, val order: SQLOrderingSpecification)