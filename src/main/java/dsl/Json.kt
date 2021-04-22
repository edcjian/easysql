package dsl

import expr.Query
import expr.QueryJson

fun Query.json(value: Any, operator: String = "->"): QueryJson {
    val chain = when (value) {
        is Int -> "[$value]"
        is String -> ".$value"
        else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
    }
    return QueryJson(this, this, operator, value, "$$chain")
}

fun Query.jsonText(value: Any): QueryJson {
    return json(value, "->>")
}

fun QueryJson.json(value: Any, operator: String = "->"): QueryJson {
    val chain = when (value) {
        is Int -> "[$value]"
        is String -> ".$value"
        else -> throw TypeCastException("取Json值时，表达式右侧只支持String或Int")
    }
    return QueryJson(this, this.initQuery, operator, value, "${this.chain}$chain")
}

fun QueryJson.jsonText(value: Any): QueryJson {
    return json(value, "->>")
}