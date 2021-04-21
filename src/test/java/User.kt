import dsl.column
import expr.TableSchema

data class User(val id: Long? = 1, val name: String? = null) {
    companion object : TableSchema("user") {
        val id = column("id")
        val name = column("user_name")
    }
}

data class User1(val id: Long? = 1, val name: String? = null) {
    companion object : TableSchema("user1") {
        val id = column("id")
        val name = column("user_name")
    }
}