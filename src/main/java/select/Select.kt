package select

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.SQLLimit
import com.alibaba.druid.sql.ast.SQLOrderBy
import com.alibaba.druid.sql.ast.SQLOrderingSpecification
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr
import com.alibaba.druid.sql.ast.statement.*
import dsl.*
import expr.*


class Select(db: DB = DB.MYSQL) : SelectQuery {
    private var sqlSelect = SQLSelectQueryBlock()

    private lateinit var joinLeft: SQLTableSourceImpl

    private var dbType: DB = db

    init {
        sqlSelect.dbType = getDbType(dbType)
    }

    fun from(table: String, alias: String? = null): Select {
        val from = SQLExprTableSource(table)
        from.alias = alias
        joinLeft = from
        sqlSelect.from = from
        return this
    }

    fun from(select: SelectQuery, alias: String? = null): Select {
        val from = SQLSubqueryTableSource(select.getSelect(), alias)
        joinLeft = from
        sqlSelect.from = from
        return this
    }

    fun <T : TableSchema> from(table: T, alias: String? = null): Select {
        return from(table.tableName, alias)
    }

    fun distinct(): Select {
        sqlSelect.distionOption = 2
        return this
    }

    fun select(vararg query: Query): Select {
        query.forEach {
            val queryExpr = getQueryExpr(it)
            sqlSelect.addSelectItem(queryExpr.expr, queryExpr.alias)
        }
        return this
    }

    fun select(): Select {
        sqlSelect.addSelectItem(SQLAllColumnExpr())
        return this
    }

    fun select(vararg columns: String): Select {
        columns.forEach {
            val trim = it.trim()
            if (trim.contains("*")) {
                sqlSelect.addSelectItem(SQLAllColumnExpr())
            } else {
                val split = trim.split(" ")
                val alias = if (trim.contains(" ")) {
                    split.last()
                } else {
                    null
                }
                val column = split.first()

                if (column.contains(".")) {
                    val splitDot = column.split(".")
                    val expr = SQLPropertyExpr()
                    expr.owner = SQLIdentifierExpr(splitDot.first())
                    expr.name = splitDot.last()
                    sqlSelect.addSelectItem(expr, alias)
                } else {
                    sqlSelect.addSelectItem(column, alias)
                }
            }
        }
        return this
    }

    fun selectWithString(value: String): Select {
        val expr = SQLUtils.toSQLExpr(value)
        sqlSelect.addSelectItem(expr)
        return this
    }

    fun selectIfNull(query: Query, value: Query, alias: String? = null): Select {
        var select = ifNull(query, value, this.dbType)
        alias?.let { select = select alias it }
        return select(select)
    }

    fun <T> selectIfNull(query: Query, value: T, alias: String? = null): Select {
        var select = ifNull(query, const(value), this.dbType)
        alias?.let { select = select alias it }
        return select(select)
    }

    private fun selectStringAgg(
            function: (Query, String, DB, SQLOrderBy?, Boolean) -> Query,
            query: Query,
            separator: String = ",",
            orderBy: SQLOrderBy? = null,
            distinct: Boolean = false,
            alias: String? = null
    ): Select {
        var select = function(query, separator, this.dbType, orderBy, distinct)
        alias?.let { select = select alias it }
        return select(select)
    }

    fun selectStringAgg(
            query: Query,
            separator: String = ",",
            orderBy: SQLOrderBy? = null,
            distinct: Boolean = false,
            alias: String? = null
    ): Select {
        return selectStringAgg(::stringAgg, query, separator, orderBy, distinct, alias)
    }

    fun selectArrayAgg(
            query: Query,
            separator: String = ",",
            orderBy: SQLOrderBy? = null,
            distinct: Boolean = false,
            alias: String? = null
    ): Select {
        return selectStringAgg(::arrayAgg, query, separator, orderBy, distinct, alias)
    }

    fun where(query: Query): Select {
        sqlSelect.addCondition(getQueryExpr(query).expr)
        return this
    }

    fun where(test: () -> Boolean, query: Query): Select {
        if (test()) {
            where(query)
        }
        return this
    }

    fun where(test: Boolean, query: Query): Select {
        if (test) {
            where(query)
        }
        return this
    }

    fun having(query: Query): Select {
        sqlSelect.addHaving(getQueryExpr(query).expr)
        return this
    }

    private fun orderBy(specification: SQLOrderingSpecification, vararg columns: Query) {
        val order = SQLOrderBy()
        columns.forEach {
            val item = SQLSelectOrderByItem()
            item.expr = getQueryExpr(it).expr
            item.type = specification
            order.addItem(item)
        }
        sqlSelect.addOrderBy(order)
    }

    fun orderByAsc(vararg columns: String): Select {
        orderBy(SQLOrderingSpecification.ASC, *columns.map { column(it) }.toTypedArray())
        return this
    }

    fun orderByAsc(vararg columns: Query): Select {
        orderBy(SQLOrderingSpecification.ASC, *columns)
        return this
    }

    fun orderByDesc(vararg columns: String): Select {
        orderBy(SQLOrderingSpecification.DESC, *columns.map { column(it) }.toTypedArray())
        return this
    }

    fun orderByDesc(vararg columns: Query): Select {
        orderBy(SQLOrderingSpecification.DESC, *columns)
        return this
    }

    fun limit(count: Int, offset: Int): Select {
        val sqlLimit = SQLLimit(SQLIntegerExpr(offset), SQLIntegerExpr(count))
        sqlSelect.limit = sqlLimit
        return this
    }

    fun limit(count: Int): Select {
        return limit(count, 0)
    }

    fun groupBy(vararg columns: Query): Select {
        var group = sqlSelect.groupBy
        if (group == null) {
            group = SQLSelectGroupByClause()
        }
        columns.forEach {
            val expr = getQueryExpr(it).expr
            group.addItem(expr)
        }
        sqlSelect.groupBy = group
        return this
    }

    fun groupBy(vararg columns: String): Select {
        val query = columns.map { QueryColumn(it) }.toTypedArray()
        return groupBy(*query)
    }

    // todo 后续抽象出来一个高阶函数放到dsl中，Select实现高阶函数内容
    private fun join(
            table: String,
            alias: String? = null,
            on: Query,
            joinType: SQLJoinTableSource.JoinType
    ): Select {
        val join = SQLJoinTableSource()
        join.left = joinLeft
        val right = SQLExprTableSource(table)
        right.alias = alias
        join.right = right
        join.joinType = joinType
        val condition = getQueryExpr(on).expr
        join.condition = condition
        sqlSelect.from = join
        joinLeft = join
        return this
    }

    private fun join(
            table: SelectQuery,
            alias: String? = null,
            on: Query,
            joinType: SQLJoinTableSource.JoinType
    ): Select {
        val join = SQLJoinTableSource()
        join.left = joinLeft
        val tableSource = SQLSubqueryTableSource(table.getSelect())
        tableSource.alias = alias
        join.right = tableSource
        join.joinType = joinType
        val condition = getQueryExpr(on).expr
        join.condition = condition
        sqlSelect.from = join
        joinLeft = join
        return this
    }

    fun join(table: String, alias: String? = null, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.JOIN)
    }

    fun join(table: TableSchema, alias: String? = null, on: Query): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.JOIN)
    }

    fun join(table: SelectQuery, alias: String?, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.JOIN)
    }

    fun leftJoin(table: String, alias: String? = null, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN)
    }

    fun leftJoin(table: TableSchema, alias: String? = null, on: Query): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN)
    }

    fun leftJoin(table: SelectQuery, alias: String?, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN)
    }

    fun rightJoin(table: String, alias: String? = null, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN)
    }

    fun rightJoin(table: TableSchema, alias: String? = null, on: Query): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN)
    }

    fun rightJoin(table: SelectQuery, alias: String?, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN)
    }

    fun innerJoin(table: String, alias: String? = null, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.INNER_JOIN)
    }

    fun innerJoin(table: TableSchema, alias: String? = null, on: Query): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.INNER_JOIN)
    }

    fun innerJoin(table: SelectQuery, alias: String?, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.INNER_JOIN)
    }

    fun crossJoin(table: String, alias: String? = null, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.CROSS_JOIN)
    }

    fun crossJoin(table: TableSchema, alias: String? = null, on: Query): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.CROSS_JOIN)
    }

    fun crossJoin(table: SelectQuery, alias: String?, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.CROSS_JOIN)
    }

    fun fullJoin(table: String, alias: String? = null, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.FULL_OUTER_JOIN)
    }

    fun fullJoin(table: TableSchema, alias: String? = null, on: Query): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.FULL_OUTER_JOIN)
    }

    fun fullJoin(table: SelectQuery, alias: String?, on: Query): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.FULL_OUTER_JOIN)
    }

    override fun sql(): String {
        if (sqlSelect.selectList.isEmpty()) {
            select()
        }
        return SQLUtils.toSQLString(sqlSelect, sqlSelect.dbType)
    }

    override fun getSelect(): SQLSelectQuery {
        return this.sqlSelect
    }

    override fun getDbType(): DB {
        return this.dbType
    }
}