package dsl

import com.alibaba.druid.sql.ast.statement.SQLUnionOperator
import query.select.SelectQuery
import query.select.UnionSelect

infix fun SelectQuery.union(select: SelectQuery): SelectQuery {
    return UnionSelect(this, SQLUnionOperator.UNION, select, this.getDbType())
}

infix fun SelectQuery.unionAll(select: SelectQuery): SelectQuery {
    return UnionSelect(this, SQLUnionOperator.UNION_ALL, select, this.getDbType())
}