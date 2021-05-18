package database

import expr.DB
import expr.Query
import expr.TableSchema
import query.delete.Delete
import query.insert.Insert
import query.select.Select
import query.update.Update

class DBConnection(source: javax.sql.DataSource, var db: DB) {
    private var dataSource: javax.sql.DataSource

    init {
        dataSource = source
    }

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
        val update = Update(db, this)
        update.update(table)
        return update
    }

    infix fun update(table: TableSchema): Update {
        val update = Update(db, this)
        update.update(table)
        return update
    }

    infix fun insert(table: TableSchema): Insert {
        val insert = Insert(db, this)
        insert.into(table)
        return insert
    }

    infix fun delete(table: String): Delete {
        val delete = Delete(db, this)
        delete.from(table)
        return delete
    }

    infix fun delete(table: TableSchema): Delete {
        val delete = Delete(db, this)
        delete.from(table)
        return delete
    }
}