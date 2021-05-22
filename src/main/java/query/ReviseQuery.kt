package query

abstract class ReviseQuery : BasedQuery {
    open fun exec(): Int {
        val result = database.exec(conn!!, this.sql())
        if (!isTransaction) {
            conn!!.close()
        }
        return result
    }

    override fun toString(): String {
        return sql()
    }
}