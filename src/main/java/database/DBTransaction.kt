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
import java.sql.SQLException

class DBTransaction(override var db: DB, var conn: Connection? = null) : DataBaseImpl() {
    override val isTransaction: Boolean = true

    override fun getConnection(): Connection {
        return this.conn!!
    }
}