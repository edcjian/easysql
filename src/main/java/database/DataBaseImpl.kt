package database

import expr.Query
import expr.TableSchema
import query.ddl.DropIndex
import query.ddl.DropTable
import query.delete.Delete
import query.insert.Insert
import query.insert.NativeInsert
import query.select.NativeSelect
import query.select.Select
import query.truncate.Truncate
import query.update.Update
import visitor.checkOLAP

abstract class DataBaseImpl : DataBase {
    fun select(vararg columns: String): Select {
        val select = Select(db, getConnection(), isTransaction)
        select.select(*columns)
        return select
    }

    infix fun select(query: Query): Select {
        val select = Select(db, getConnection(), isTransaction)
        select.invoke(query)
        return select
    }

    infix fun select(query: List<Query>): Select {
        val select = Select(db, getConnection(), isTransaction)
        select.invoke(query)
        return select
    }

    fun select(vararg query: Query): Select {
        val select = Select(db, getConnection(), isTransaction)
        select.select(*query)
        return select
    }

    fun select(): Select {
        return Select(db, getConnection(), isTransaction)
    }

    fun nativeSelect(sql: String): NativeSelect {
        return NativeSelect(db, sql, getConnection(), isTransaction)
    }

    infix fun update(table: String): Update {
        checkOLAP(this.db)

        val update = Update(db, getConnection(), isTransaction)
        update.update(table)
        return update
    }

    infix fun update(table: TableSchema): Update {
        checkOLAP(this.db)

        val update = Update(db, getConnection(), isTransaction)
        update.update(table)
        return update
    }

    infix fun insert(table: String): NativeInsert {
        checkOLAP(this.db)

        val insert = NativeInsert(db, getConnection(), isTransaction)
        insert.into(table)
        return insert
    }

    infix fun insert(table: TableSchema): Insert {
        checkOLAP(this.db)

        val insert = Insert(db, getConnection(), isTransaction)
        insert.into(table)
        return insert
    }

    infix fun delete(table: String): Delete {
        checkOLAP(this.db)

        val delete = Delete(db, getConnection(), isTransaction)
        delete.from(table)
        return delete
    }

    infix fun delete(table: TableSchema): Delete {
        checkOLAP(this.db)

        val delete = Delete(db, getConnection(), isTransaction)
        delete.from(table)
        return delete
    }

    infix fun truncate(table: String): Truncate {
        checkOLAP(this.db)

        val truncate = Truncate(db, getConnection(), isTransaction)
        truncate.truncate(table)
        return truncate
    }

    infix fun truncate(table: TableSchema): Truncate {
        checkOLAP(this.db)

        val truncate = Truncate(db, getConnection(), isTransaction)
        truncate.truncate(table)
        return truncate
    }

    infix fun dropTable(table: String): DropTable {
        val dropTable = DropTable(db, getConnection(), isTransaction)
        dropTable.drop(table)
        return dropTable
    }

    infix fun dropTable(table: TableSchema): DropTable {
        val dropTable = DropTable(db, getConnection(), isTransaction)
        dropTable.drop(table)
        return dropTable
    }

    infix fun dropIndex(indexName: String): DropIndex {
        val dropIndex = DropIndex(db, getConnection(), isTransaction)
        dropIndex.drop(indexName)
        return dropIndex
    }
}