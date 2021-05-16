package util

import kotlin.reflect.KProperty
import kotlin.reflect.full.declaredMembers

inline fun <reified T> convertClassToObject() {
    val clazz = T::class
    val name = clazz.simpleName
    println("companion object : TableSchema(\"${humpToLine(name!!)}\") {")
    clazz.declaredMembers.filterIsInstance<KProperty<*>>().forEach {
        println("    val ${it.name} = column(\"${humpToLine(it.name)}\")")
    }
    println("}")
}

fun humpToLine(string: String): String {
    val builder = StringBuilder()
    string.forEachIndexed { index, it ->
        if (it.isUpperCase() && index > 0) {
            builder.append("_")
        }
        builder.append(it.toLowerCase())
    }

    return builder.toString()
}