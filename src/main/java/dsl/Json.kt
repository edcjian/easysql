package dsl

import expr.Query
import expr.QueryJson

fun Query.json(value: Any, operator: String = "->"): QueryJson {
    return QueryJson(this, operator, value)
}

fun Query.jsonText(value: Any): QueryJson {
    return json(value, "->>")
}

fun QueryJson.json(value: Any, operator: String = "->"): QueryJson {
    return QueryJson(this, operator, value)
}

fun QueryJson.jsonText(value: Any): QueryJson {
    return json(value, "->>")
}