package database

import expr.DB
import java.sql.Connection

interface DataBase {
    val db: DB

    val isTransaction: Boolean

    fun getConnection(): Connection
}