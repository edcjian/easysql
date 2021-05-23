package query.ddl

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.SQLDataType
import com.alibaba.druid.sql.ast.SQLDataTypeImpl
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr
import com.alibaba.druid.sql.ast.statement.*
import com.alibaba.druid.sql.parser.SQLParserUtils
import com.alibaba.druid.sql.visitor.VisitorFeature
import expr.DB
import query.ReviseQuery
import visitor.getDbType
import java.sql.Connection

class CreateTable(
    var db: DB = DB.MYSQL,
    override var conn: Connection? = null,
    override var isTransaction: Boolean = false
) : ReviseQuery() {
    private var sqlCreateTable = SQLCreateTableStatement()

    init {
        sqlCreateTable.dbType = getDbType(db)
    }

    infix fun create(table: String): CreateTable {
        sqlCreateTable.name = SQLIdentifierExpr(table)
        return this
    }

    infix fun columns(columns: CreateTable.() -> Unit): CreateTable {
        columns()
        return this
    }

    fun add(column: Column): CreateTable {
        val sqlColumn = SQLColumnDefinition()
        sqlColumn.name = SQLIdentifierExpr(column.columnName)
        sqlColumn.dataType = SQLParserUtils.createExprParser(column.dataType, sqlCreateTable.dbType).parseDataType()

        if (column.isPrimaryKey) {
            val primary = SQLColumnPrimaryKey()
            sqlColumn.addConstraint(primary)
        }

        if (column.isNotNull) {
            val notNull = SQLNotNullConstraint()
            sqlColumn.addConstraint(notNull)
        }

        sqlCreateTable.addColumn(sqlColumn)

        return this
    }

    fun column(columnName: String): Column {
        return Column(columnName = columnName)
    }

    class Column(
        var columnName: String = "",
        var dataType: String = "",
        var isPrimaryKey: Boolean = false,
        var isNotNull: Boolean = false
    ) {
        fun dataType(dataType: String): Column {
            this.dataType = dataType
            return this
        }

        fun primary(): Column {
            this.isPrimaryKey = true
            return this
        }

        fun notNull(): Column {
            this.isNotNull = true
            return this
        }
    }

    override fun sql(): String {
        return SQLUtils.toSQLString(
            sqlCreateTable,
            sqlCreateTable.dbType,
            SQLUtils.FormatOption(),
            VisitorFeature.OutputNameQuote
        )
    }
}