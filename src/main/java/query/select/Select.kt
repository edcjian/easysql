package query.select

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.SQLLimit
import com.alibaba.druid.sql.ast.SQLOrderBy
import com.alibaba.druid.sql.ast.SQLOrderingSpecification
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr
import com.alibaba.druid.sql.ast.statement.*
import com.alibaba.druid.sql.visitor.VisitorFeature
import dsl.column
import dsl.count
import expr.*
import visitor.getDbType
import visitor.getQueryExpr
import java.sql.Connection
import kotlin.reflect.KProperty
import kotlin.reflect.full.companionObjectInstance
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.declaredMembers
import kotlin.reflect.jvm.javaField


class Select(var db: DB = DB.MYSQL, override var conn: Connection? = null) : SelectQueryImpl(), Cloneable {
    private var sqlSelect = SQLSelectQueryBlock()

    private lateinit var joinLeft: SQLTableSourceImpl

    init {
        sqlSelect.addSelectItem(SQLAllColumnExpr())
        sqlSelect.dbType = getDbType(db)
    }

    fun getSqlSelect() = this.sqlSelect

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

    infix fun from(select: SelectQuery): Select {
        return from(select, null)
    }

    fun <T : TableSchema> from(table: T, alias: String? = null): Select {
        return from(table.tableName, alias)
    }

    infix fun <T : TableSchema> from(table: T): Select {
        return from(table.tableName, null)
    }

    infix fun alias(name: String): Select {
        val from = this.sqlSelect.from
        if (from is SQLJoinTableSource) {
            from.right.alias = name
        } else {
            from.alias = name
        }

        return this
    }

    fun distinct(): Select {
        sqlSelect.distionOption = 2
        return this
    }

    fun select(vararg query: Query): Select {
        if (this.sqlSelect.selectList.size == 1 && this.sqlSelect.selectList[0].expr is SQLAllColumnExpr) {
            this.sqlSelect.selectList.clear()
        }
        query.forEach {
            val queryExpr = getQueryExpr(it, this.db)
            sqlSelect.addSelectItem(queryExpr.expr, queryExpr.alias)
        }
        return this
    }

    infix operator fun invoke(query: List<Query>): Select {
        return select(*query.toTypedArray())
    }

    infix operator fun invoke(query: Query): Select {
        return select(*arrayOf(query))
    }

    infix fun select(query: List<Query>): Select {
        return select(*query.toTypedArray())
    }

    infix fun select(query: Query): Select {
        return select(*arrayOf(query))
    }

    infix fun selectDistinct(query: List<Query>): Select {
        distinct()
        return select(*query.toTypedArray())
    }

    infix fun selectDistinct(query: Query): Select {
        distinct()
        return select(*arrayOf(query))
    }

    fun select(): Select {
        sqlSelect.addSelectItem(SQLAllColumnExpr())
        return this
    }

    fun select(vararg columns: String): Select {
        if (this.sqlSelect.selectList.size == 1 && this.sqlSelect.selectList[0].expr is SQLAllColumnExpr) {
            this.sqlSelect.selectList.clear()
        }
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

    infix fun where(query: Query): Select {
        sqlSelect.addCondition(getQueryExpr(query, this.db).expr)
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

    infix fun having(query: Query): Select {
        sqlSelect.addHaving(getQueryExpr(query, this.db).expr)
        return this
    }

    private fun orderBy(specification: SQLOrderingSpecification, vararg columns: Query) {
        val order = SQLOrderBy()
        columns.forEach {
            val item = SQLSelectOrderByItem()
            item.expr = getQueryExpr(it, this.db).expr
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

    infix fun orderByAsc(columns: List<Query>): Select {
        return orderByAsc(*columns.toTypedArray())
    }

    infix fun orderByAsc(column: Query): Select {
        return orderByAsc(*arrayOf(column))
    }

    fun orderByDesc(vararg columns: String): Select {
        orderBy(SQLOrderingSpecification.DESC, *columns.map { column(it) }.toTypedArray())
        return this
    }

    fun orderByDesc(vararg columns: Query): Select {
        orderBy(SQLOrderingSpecification.DESC, *columns)
        return this
    }

    infix fun orderByDesc(columns: List<Query>): Select {
        return orderByDesc(*columns.toTypedArray())
    }

    infix fun orderByDesc(column: Query): Select {
        return orderByDesc(*arrayOf(column))
    }

    fun limit(count: Int, offset: Int): Select {
        val sqlLimit = SQLLimit(SQLIntegerExpr(offset), SQLIntegerExpr(count))
        sqlSelect.limit = sqlLimit
        return this
    }

    infix fun limit(count: Int): Select {
        return limit(count, 0)
    }

    infix fun offset(offset: Int): Select {
        this.sqlSelect.limit.offset = SQLIntegerExpr(offset)
        return this
    }

    fun groupBy(vararg columns: Query): Select {
        var group = sqlSelect.groupBy
        if (group == null) {
            group = SQLSelectGroupByClause()
        }
        columns.forEach {
            val expr = getQueryExpr(it, this.db).expr
            group.addItem(expr)
        }
        sqlSelect.groupBy = group
        return this
    }

    infix fun groupBy(column: Query): Select {
        return groupBy(*arrayOf(column))
    }

    infix fun groupBy(columns: List<Query>): Select {
        return groupBy(*columns.toTypedArray())
    }

    fun groupBy(vararg columns: String): Select {
        val query = columns.map { QueryColumn(it) }.toTypedArray()
        return groupBy(*query)
    }

    private fun join(
        table: String,
        alias: String? = null,
        on: Query?,
        joinType: SQLJoinTableSource.JoinType
    ): Select {
        val join = SQLJoinTableSource()
        join.left = joinLeft
        val right = SQLExprTableSource(table)
        right.alias = alias
        join.right = right
        join.joinType = joinType
        if (on != null) {
            val condition = getQueryExpr(on, this.db).expr
            join.condition = condition
        }
        sqlSelect.from = join
        joinLeft = join
        return this
    }

    private fun join(
        table: SelectQuery,
        alias: String? = null,
        on: Query?,
        joinType: SQLJoinTableSource.JoinType
    ): Select {
        val join = SQLJoinTableSource()
        join.left = joinLeft
        val tableSource = SQLSubqueryTableSource(table.getSelect())
        tableSource.alias = alias
        join.right = tableSource
        join.joinType = joinType
        if (on != null) {
            val condition = getQueryExpr(on, this.db).expr
            join.condition = condition
        }
        sqlSelect.from = join
        joinLeft = join
        return this
    }

    infix fun on(on: Query): Select {
        val from = this.sqlSelect.from
        if (from is SQLJoinTableSource) {
            from.condition = getQueryExpr(on, this.db).expr
        }
        return this
    }

    fun join(table: String, alias: String? = null, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.JOIN)
    }

    fun join(table: TableSchema, alias: String? = null, on: Query? = null): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.JOIN)
    }

    fun join(table: SelectQuery, alias: String?, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.JOIN)
    }

    infix fun join(table: TableSchema): Select {
        return join(table.tableName, null, null, SQLJoinTableSource.JoinType.JOIN)
    }

    infix fun join(table: SelectQuery): Select {
        return join(table, null, null, SQLJoinTableSource.JoinType.JOIN)
    }

    fun leftJoin(table: String, alias: String? = null, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN)
    }

    fun leftJoin(table: TableSchema, alias: String? = null, on: Query? = null): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN)
    }

    fun leftJoin(table: SelectQuery, alias: String?, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN)
    }

    infix fun leftJoin(table: TableSchema): Select {
        return join(table.tableName, null, null, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN)
    }

    infix fun leftJoin(table: SelectQuery): Select {
        return join(table, null, null, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN)
    }

    fun rightJoin(table: String, alias: String? = null, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN)
    }

    fun rightJoin(table: TableSchema, alias: String? = null, on: Query? = null): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN)
    }

    fun rightJoin(table: SelectQuery, alias: String?, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN)
    }

    infix fun rightJoin(table: TableSchema): Select {
        return join(table.tableName, null, null, SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN)
    }

    infix fun rightJoin(table: SelectQuery): Select {
        return join(table, null, null, SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN)
    }

    fun innerJoin(table: String, alias: String? = null, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.INNER_JOIN)
    }

    fun innerJoin(table: TableSchema, alias: String? = null, on: Query? = null): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.INNER_JOIN)
    }

    fun innerJoin(table: SelectQuery, alias: String?, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.INNER_JOIN)
    }

    infix fun innerJoin(table: TableSchema): Select {
        return join(table.tableName, null, null, SQLJoinTableSource.JoinType.INNER_JOIN)
    }

    infix fun innerJoin(table: SelectQuery): Select {
        return join(table, null, null, SQLJoinTableSource.JoinType.INNER_JOIN)
    }

    fun crossJoin(table: String, alias: String? = null, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.CROSS_JOIN)
    }

    fun crossJoin(table: TableSchema, alias: String? = null, on: Query? = null): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.CROSS_JOIN)
    }

    fun crossJoin(table: SelectQuery, alias: String?, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.CROSS_JOIN)
    }

    infix fun crossJoin(table: TableSchema): Select {
        return join(table.tableName, null, null, SQLJoinTableSource.JoinType.CROSS_JOIN)
    }

    infix fun crossJoin(table: SelectQuery): Select {
        return join(table, null, null, SQLJoinTableSource.JoinType.CROSS_JOIN)
    }

    fun fullJoin(table: String, alias: String? = null, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.FULL_OUTER_JOIN)
    }

    fun fullJoin(table: TableSchema, alias: String? = null, on: Query? = null): Select {
        return join(table.tableName, alias, on, SQLJoinTableSource.JoinType.FULL_OUTER_JOIN)
    }

    fun fullJoin(table: SelectQuery, alias: String?, on: Query? = null): Select {
        return join(table, alias, on, SQLJoinTableSource.JoinType.FULL_OUTER_JOIN)
    }

    infix fun fullJoin(table: TableSchema): Select {
        return join(table.tableName, null, null, SQLJoinTableSource.JoinType.FULL_OUTER_JOIN)
    }

    infix fun fullJoin(table: SelectQuery): Select {
        return join(table, null, null, SQLJoinTableSource.JoinType.FULL_OUTER_JOIN)
    }

    override fun sql(): String {
        if (sqlSelect.selectList.isEmpty()) {
            select()
        }
        return SQLUtils.toSQLString(
            sqlSelect,
            sqlSelect.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }

    override fun getSelect(): SQLSelectQuery {
        return this.sqlSelect
    }

    override fun getDbType(): DB {
        return this.db
    }

    fun <T : Any> find(clazz: Class<T>): T? {
        val selectCopy = this.sqlSelect.clone()
        val limit = SQLLimit()
        limit.offset = selectCopy.limit.offset
        limit.setRowCount(1)
        selectCopy.limit = limit
        val sql = SQLUtils.toSQLString(
            selectCopy,
            sqlSelect.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )

        val list = database.query(conn!!, sql)
        if (list.isEmpty()) {
            return null
        }

        val map = list[0]

        val companion = clazz.kotlin.companionObjectInstance ?: throw Exception("实体类需要添加伴生对象")
        val companionClass = companion::class
        val columns = companionClass.declaredMemberProperties
            .map { it.getter.call(companion) to it.name }
            .filter { it.first is QueryTableColumn }
            .map { (it.first as QueryTableColumn).column to it.second }
            .toMap()

        val rowClass = clazz
        val row = rowClass.newInstance()

        columns.forEach { column ->
            val fieldName = column.value
            val field = (rowClass.kotlin.declaredMembers.find { it.name == fieldName } as KProperty).javaField
            field?.isAccessible = true
            field?.set(row, map[column.key])
        }

        return row
    }

    inline fun <reified T> find(): T? {
        val selectCopy = getSqlSelect().clone()
        val limit = SQLLimit()
        limit.offset = selectCopy.limit.offset
        limit.setRowCount(1)
        selectCopy.limit = limit
        val sql = SQLUtils.toSQLString(
            selectCopy,
            getSqlSelect().dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )

        val list = database.query(conn!!, sql)
        if (list.isEmpty()) {
            return null
        }

        val map = list[0]

        val companion = T::class.companionObjectInstance ?: throw Exception("实体类需要添加伴生对象")
        val companionClass = companion::class
        val columns = companionClass.declaredMemberProperties
            .map { it.getter.call(companion) to it.name }
            .filter { it.first is QueryTableColumn }
            .map { (it.first as QueryTableColumn).column to it.second }
            .toMap()

        val rowClass = T::class
        val row = rowClass.java.newInstance()

        columns.forEach { column ->
            val fieldName = column.value
            val field = (rowClass.declaredMembers.find { it.name == fieldName } as KProperty).javaField
            field?.isAccessible = true
            field?.set(row, map[column.key])
        }

        return row
    }

    fun findMap(): Map<String, Any>? {
        val selectCopy = this.sqlSelect.clone()
        val limit = SQLLimit()
        limit.offset = selectCopy.limit.offset
        limit.setRowCount(1)
        selectCopy.limit = limit
        val sql = SQLUtils.toSQLString(
            selectCopy,
            sqlSelect.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )

        val list = database.query(conn!!, sql)
        if (list.isEmpty()) {
            return null
        }

        return list[0]
    }

    override fun fetchCount(): Long {
        val selectCopy = this.sqlSelect.clone()
        selectCopy.limit = null
        selectCopy.selectList.clear()
        selectCopy.addSelectItem(getQueryExpr(count(), this.db).expr, "count")

        val sql = SQLUtils.toSQLString(
            selectCopy,
            sqlSelect.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )

        val result = database.query(conn!!, sql)
        return result[0]["count"] as Long
    }

    override fun exist(): Boolean {
        val result = this.findMap()
        return !result.isNullOrEmpty()
    }
}