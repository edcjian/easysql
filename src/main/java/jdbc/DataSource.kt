package jdbc

import com.alibaba.druid.pool.DruidDataSource
import expr.DB
import expr.Query
import query.select.Select

class DataSource(url: String, userName: String, password: String, driver: String, database: DB) {
    private var dataSource: DruidDataSource = DruidDataSource()
    private var db: DB = database

    init {
        dataSource.url = url
        dataSource.username = userName
        dataSource.password = password
        dataSource.driverClassName = driver
    }

    fun getDataSource() = dataSource

    infix fun select(query: Query): Select {
        val select = Select(db, this)
        select.invoke(query)
        return select
    }

    infix fun select(query: List<Query>): Select {
        val select = Select(db, this)
        select.invoke(query)
        return select
    }

    fun select(vararg query: Query): Select {
        val select = Select(db, this)
        select.select(*query)
        return select
    }

    fun select(): Select {
        return Select(db, this)
    }
}