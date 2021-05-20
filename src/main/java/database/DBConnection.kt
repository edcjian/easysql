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
import java.sql.Connection
import java.sql.Connection.TRANSACTION_READ_COMMITTED
import java.sql.SQLException
import javax.sql.DataSource

class DBConnection(source: DataSource, override val db: DB) : DataBaseImpl() {
    override val isTransaction: Boolean = false

    private var dataSource: DataSource = source

    fun getDataSource() = dataSource

    inline fun transaction(isolation: Int = TRANSACTION_READ_COMMITTED, query: DBTransaction.() -> Unit) {
        checkOLAP(this.db)

        val conn = this.getDataSource().connection
        conn.autoCommit = false
        conn.transactionIsolation = isolation
        try {
            query(DBTransaction(this.db, conn))
            conn.commit()
        } catch (e: Exception) {
            e.printStackTrace()
            conn.rollback()
        } finally {
            conn.autoCommit = true
            conn.close()
        }
    }

    override fun getConnection(): Connection {
        return this.dataSource.connection
    }
}