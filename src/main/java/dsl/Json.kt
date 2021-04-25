package dsl

import expr.Query
import expr.QueryJson

infix fun Query.json(value: Any): QueryJson {
    return QueryJson(this, "->", value)
}

infix fun Query.jsonText(value: Any): QueryJson {
    return QueryJson(this, "->>", value)
}

infix fun QueryJson.json(value: Any): QueryJson {
    return QueryJson(this, "->", value)
}

infix fun QueryJson.jsonText(value: Any): QueryJson {
    return QueryJson(this, "->>", value)
}