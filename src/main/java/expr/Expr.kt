package expr

import com.alibaba.druid.sql.ast.SQLExpr
import com.alibaba.druid.sql.ast.SQLOrderBy
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption
import select.SelectQuery

abstract class Query(open var alias: String?) {
    constructor() : this(null)

    operator fun plus(query: Query): QueryBinary {
        return QueryBinary(this, "+", query)
    }

    operator fun minus(query: Query): QueryBinary {
        return QueryBinary(this, "-", query)
    }

    operator fun times(query: Query): QueryBinary {
        return QueryBinary(this, "*", query)
    }

    operator fun div(query: Query): QueryBinary {
        return QueryBinary(this, "/", query)
    }

    operator fun rem(query: Query): QueryBinary {
        return QueryBinary(this, "%", query)
    }
}

data class QueryExpr(val expr: SQLExpr, override var alias: String?) : Query() {
    constructor(expr: SQLExpr) : this(expr, null)
}

data class QueryColumn(val column: String, override var alias: String?) : Query() {
    constructor(column: String) : this(column, null)
}

data class QueryExprFunction(val name: String, val args: List<Query>, override var alias: String? = null) : Query()

data class QueryAggFunction(
    val name: String,
    val args: List<Query>,
    val option: SQLAggregateOption? = null,
    val attributes: Map<String, Query>? = null,
    val orderBy: SQLOrderBy? = null,
    override var alias: String? = null
) : Query()

data class QueryConst<T>(val value: T, override var alias: String?) : Query() {
    constructor(value: T) : this(value, null)
}

data class QueryBinary(val left: Query, val operator: String, val right: Query?, override var alias: String?) :
    Query() {
    constructor(left: Query, operator: String, right: Query?) : this(left, operator, right, null)
}

data class QuerySub(val selectQuery: SelectQuery, override var alias: String?) : Query() {
    constructor(selectQuery: SelectQuery) : this(selectQuery, null)
}

data class QueryCase<T>(val conditions: List<CaseBranch<T>>, val default: T?, override var alias: String?) : Query() {
    constructor(conditions: List<CaseBranch<T>>, default: T) : this(conditions, default, null)

    constructor(conditions: List<CaseBranch<T>>) : this(conditions, null, null)
}

data class QueryJson(
    val query: Query,
    val operator: String,
    val value: Any,
    val db: DB,
    val chain: String? = null,
    override var alias: String? = null
) : Query()

data class CaseBranch<T>(val query: QueryExpr, val then: T)

enum class DB {
    MYSQL, ORACLE, PGSQL, HIVE
}