package query

import java.sql.Connection

interface BasedQuery {
    var conn: Connection?

    var isTransaction: Boolean

    fun sql(): String
}