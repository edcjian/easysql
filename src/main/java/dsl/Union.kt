package dsl

import com.alibaba.druid.sql.ast.statement.SQLUnionOperator
import query.select.SelectQuery
import query.select.SelectQueryImpl
import query.select.UnionSelect

infix fun SelectQuery.union(select: SelectQuery): UnionSelect {
    return UnionSelect(this, SQLUnionOperator.UNION, select, this.getDbType())
}

infix fun SelectQuery.unionAll(select: SelectQuery): UnionSelect {
    return UnionSelect(this, SQLUnionOperator.UNION_ALL, select, this.getDbType())
}