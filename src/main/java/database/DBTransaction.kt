package database

import expr.DB
import java.sql.Connection

class DBTransaction(override var db: DB, var conn: Connection? = null) : DataBaseImpl() {
    override val isTransaction: Boolean = true

    override fun getConnection(): Connection {
        return this.conn!!
    }
}