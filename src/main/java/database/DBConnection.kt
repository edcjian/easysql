package database

import expr.DB
import expr.Query
import expr.TableSchema
import query.delete.Delete
import query.insert.Insert
import query.select.Select
import query.truncate.Truncate
import query.update.Update
import visitor.checkOLAP
import java.sql.SQLException
import javax.sql.DataSource

class DBConnection(source: DataSource, var db: DB) {
    private var dataSource: DataSource = source

    fun getDataSource() = dataSource

    infix fun select(query: Query): Select {
        val select = Select(db, this.dataSource.connection)
        select.invoke(query)
        return select
    }

    infix fun select(query: List<Query>): Select {
        val select = Select(db, this.dataSource.connection)
        select.invoke(query)
        return select
    }

    fun select(vararg query: Query): Select {
        val select = Select(db, this.dataSource.connection)
        select.select(*query)
        return select
    }

    fun select(): Select {
        return Select(db, this.dataSource.connection)
    }

    infix fun update(table: String): Update {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val update = Update(db, this.dataSource.connection)
        update.update(table)
        return update
    }

    infix fun update(table: TableSchema): Update {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val update = Update(db, this.dataSource.connection)
        update.update(table)
        return update
    }

    infix fun insert(table: TableSchema): Insert {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val insert = Insert(db, this.dataSource.connection)
        insert.into(table)
        return insert
    }

    infix fun delete(table: String): Delete {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val delete = Delete(db, this.dataSource.connection)
        delete.from(table)
        return delete
    }

    infix fun delete(table: TableSchema): Delete {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val delete = Delete(db, this.dataSource.connection)
        delete.from(table)
        return delete
    }

    infix fun truncate(table: String): Truncate {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val truncate = Truncate(db, this.dataSource.connection)
        truncate.truncate(table)
        return truncate
    }

    infix fun truncate(table: TableSchema): Truncate {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val truncate = Truncate(db, this.dataSource.connection)
        truncate.truncate(table)
        return truncate
    }

    infix fun transaction(query: (DBConnection) -> Unit) {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        query(this)
    }
}