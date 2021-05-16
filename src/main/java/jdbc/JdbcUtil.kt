package jdbc

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

fun query(conn: Connection, sql: String): List<Map<String, Any>> {
    var stmt: Statement? = null
    var rs: ResultSet? = null
    val result = mutableListOf<Map<String, Any>>()

    try {
        stmt = conn.createStatement()
        rs = stmt.executeQuery(sql)
        val metadata = rs.metaData

        while (rs.next()) {
            val rowMap = mutableMapOf<String, Any>()
            (1..metadata.columnCount).forEach {
                rowMap[metadata.getColumnName(it)] = rs.getObject(it)
            }
            result.add(rowMap)
        }

    } catch (e: SQLException) {
        e.printStackTrace()
    } finally {
        conn.close()
        stmt?.close()
        rs?.close()
    }

    return result
}