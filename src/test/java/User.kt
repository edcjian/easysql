import dsl.column
import expr.TableSchema

data class User(
    val id: Long? = 1,
    val name: String? = null,
    val gender: Int? = 1,
    val age: Int? = 1,
    val jsonInfo: String? = null,
    val ids: String? = null
) {
    companion object : TableSchema("user") {
        val id = column("id")
        val name = column("user_name")
        val gender = column("gender")
        val age = column("age")
        val jsonInfo = column("json_info")
        val ids = column("ids")
    }
}

data class User1(val id: Long? = 1, val name: String? = null) {
    companion object : TableSchema("user1") {
        val id = column("id")
        val name = column("user_name")
    }
}