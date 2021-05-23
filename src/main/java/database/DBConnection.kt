package database

import expr.DB
import visitor.checkOLAP
import java.sql.Connection
import javax.sql.DataSource

class DBConnection(source: DataSource, override val db: DB) : DataBaseImpl() {
    override val isTransaction: Boolean = false

    private var dataSource: DataSource = source

    inline fun transaction(isolation: Int? = null, query: DBTransaction.() -> Unit) {
        checkOLAP(this.db)

        val conn = getConnection()
        conn.autoCommit = false
        isolation?.let { conn.transactionIsolation = it }

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