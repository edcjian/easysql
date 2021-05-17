import dsl.column
import expr.TableSchema

data class User(
    var id: Long? = 1,
    var name: String? = null,
    var gender: Int? = 1,
    var age: Int? = 1,
    var jsonInfo: String? = null,
    var ids: String? = null
) {
    companion object : TableSchema("user") {
        val id = column("id").incr
        val name = column("user_name")
        val gender = column("gender")
        val age = column("age")
        val jsonInfo = column("json_info")
        val ids = column("ids")
    }
}

data class User1(var id: Long? = null, var name: String? = null) {
    companion object : TableSchema("user1") {
        val id = column("id")
        val name = column("user_name")
    }
}