package database

import expr.DB
import expr.Query
import expr.TableSchema
import query.delete.Delete
import query.insert.Insert
import query.select.Select
import query.update.Update
import visitor.checkOLAP
import java.sql.SQLException
import javax.sql.DataSource

class DBConnection(source: DataSource, var db: DB) {
    private var dataSource: DataSource = source

    fun getDataSource() = dataSource

    infix fun select(query: Query): Select {
        val select = Select(db, this)
        select.invoke(query)
        return select
    }

    infix fun select(query: List<Query>): Select {
        val select = Select(db, this)
        select.invoke(query)
        return select
    }

    fun select(vararg query: Query): Select {
        val select = Select(db, this)
        select.select(*query)
        return select
    }

    fun select(): Select {
        return Select(db, this)
    }

    infix fun update(table: String): Update {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val update = Update(db, this)
        update.update(table)
        return update
    }

    infix fun update(table: TableSchema): Update {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val update = Update(db, this)
        update.update(table)
        return update
    }

    infix fun insert(table: TableSchema): Insert {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val insert = Insert(db, this)
        insert.into(table)
        return insert
    }

    infix fun delete(table: String): Delete {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val delete = Delete(db, this)
        delete.from(table)
        return delete
    }

    infix fun delete(table: TableSchema): Delete {
        if (checkOLAP(this.db)) {
            throw SQLException("分析型数据库不支持此操作")
        }
        val delete = Delete(db, this)
        delete.from(table)
        return delete
    }
}