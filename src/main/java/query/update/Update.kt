package query.update

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import expr.Query
import expr.TableSchema
import jdbc.DataSource
import visitor.getDbType
import visitor.getExpr
import visitor.getQueryExpr

class Update(var db: DB = DB.MYSQL, var dataSource: DataSource? = null) {
    private var sqlUpdate = SQLUpdateStatement()

    init {
        sqlUpdate.dbType = getDbType(db)
    }

    fun update(table: String): Update {
        this.sqlUpdate.tableSource = SQLExprTableSource(table)
        return this
    }

    infix fun update(table: TableSchema): Update {
        return update(table.tableName)
    }

    infix fun <T : Any> set(item: Pair<Query, T>): Update {
        val (column, value) = item
        val updateItem = SQLUpdateSetItem()
        updateItem.column = getQueryExpr(column, this.db).expr
        updateItem.value = getExpr(value)
        this.sqlUpdate.addItem(updateItem)

        return this
    }

    infix fun set(items: List<Pair<Query, Any>>): Update {
        items.forEach {
            val (column, value) = it
            val updateItem = SQLUpdateSetItem()
            updateItem.column = getQueryExpr(column, this.db).expr
            updateItem.value = getExpr(value)
            this.sqlUpdate.addItem(updateItem)
        }

        return this
    }

    fun set(vararg items: Pair<Query, Any>): Update {
        items.forEach {
            val (column, value) = it
            val updateItem = SQLUpdateSetItem()
            updateItem.column = getQueryExpr(column, this.db).expr
            updateItem.value = getExpr(value)
            this.sqlUpdate.addItem(updateItem)
        }

        return this
    }

    infix fun where(condition: Query): Update {
        this.sqlUpdate.addCondition(getQueryExpr(condition, this.db).expr)
        return this
    }

    fun where(test: () -> Boolean, condition: Query): Update {
        if (test()) {
            where(condition)
        }

        return this
    }

    fun where(test: Boolean, condition: Query): Update {
        if (test) {
            where(condition)
        }

        return this
    }

    fun sql(): String {
        return SQLUtils.toSQLString(
            sqlUpdate,
            sqlUpdate.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }

    fun exec(): Int {
        val conn = this.dataSource!!.getDataSource().connection
        return jdbc.queryCount(conn, this.sql())
    }
}