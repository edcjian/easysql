import com.alibaba.druid.sql.visitor.functions.IfNull
import dsl.*
import expr.DB
import select.Select

object TestSql {
    @JvmStatic
    fun main(args: Array<String>) {
//                val sql = Select()
//            .from("table")
//            .distinct()
//            .select(
//                "a" alias "col1",
//                "b" alias "col2",
//                ((column("a") + count("b")) * const(2) alias "col3")
//            ).where((column("a") eq 1) and (column("b") eq "2"))
//            .groupBy("a")
//            .orderByAsc("a")
//            .sql()
//        println(sql)
//
//        val a = column("a")
//        val case = case(
//            (a eq 1) or (a eq 2) then 1,
//            a eq 3 then 1
//        ) elseIs null
//
//        val select = Select().select(count(case)).from("t")
//        println(select.sql())
//
//        val case1 = case("report_r603_20.r0042=01", 1) elseIs 0
//        println(case1.sqlString())
//
//        val case2 = count(case(column("report_r603_20.r0042") gt "1"  then "1", "report_r603_20.r0042=02" then "2") elseIs "")
//        println(case2.sqlString())
//
//        val sql1 = Select(DB.ORACLE)
//            .from("table1")
//            .select("a" alias "nation", "c" alias "gender")
//            .rightJoin("table2", on = (column("table1.id") eq column("table2.id")) and (column("table1.id") eq 1))
//            .limit(1)
//            .sql()
//        println(sql1)
//
//        val select = (column("a") + column("b")).sqlString()
//        val sql2 = Select().from("table").selectWithString(select).sql()
//        println(sql2)


//        var start = System.currentTimeMillis()
//        val case = case((column("a") eq 1) then column("a")) elseIs null
//        val select1 = Select(DB.MYSQL)
//            .select("b" alias "c1", count(case) alias "c2")
//            .from("t")
//            .groupBy("b")
//        println(select1.sql())
//        var end = System.currentTimeMillis()
//        println(end - start)
//
//        val select = Select(DB.PGSQL)
//            .from("t1")
//            .select((column("t1.a") + column("t1.b") / const(5)) alias "c")
//            .where((column("t1.d") inList listOf(1, 2)) and (column("t1.e") eq "10"))
//            .leftJoin("t2", alias = "t", on = column("t1.id") eq column("t.tid"))
//            .leftJoin("t3", alias = "tt", on = column("t1.id") eq column("tt.tid"))
//            .orderByAsc("t.f", "t.g")
//            .limit(10)
//        println(select.sql())

//        val select = Select(DB.ORACLE).select().from(
//            Select().select("a").from("t1").limit(10, 10) union
//                    Select().select("a").from("t2").limit(10, 10) unionAll
//                    Select().select("a").from("t3")
//        ).where(column("a") eq 1)
//        println(select.sql())

//        val select = Select(DB.PGSQL).select("a.id").from(Select().select().from("t").limit(10, 10), "a")
//        println(select.sql())

//        val select = Select().select().from("t").where(column("id") eq Select().select(max("id")).from("t"))
//        println(select.sql())

//        val select = Select(DB.ORACLE).select().from("t").where(
//            column("a") inList (
//                    Select().select("a").from("t1").limit(10, 10) union
//                            Select().select("a").from("t2").limit(10, 10) unionAll
//                            Select().select("a").from("t3"))
//        )
//        println(select.sql())

//        val select =
//            Select(DB.MYSQL).select().from("a")
//                .leftJoin(
//                    Select().select().from("b").limit(10, 10),
//                    alias = "b",
//                    on = column("a.id") eq (column("b.id") + const(1))
//                )
//                .rightJoin(
//                    Select().select().from("c").limit(20, 10),
//                    alias = "c",
//                    on = column("a.id") eq column("c.id")
//                )
//                .leftJoin("d", alias = "d", on = column("a.id") eq column("d.id"))
//        println(select.sql())

//        val user = ""
//        val select = Select(DB.PGSQL).select().from("t").where(user.isNotEmpty(), column("user") eq user)
//        println(select.sql())

//        val select = Select(DB.PGSQL).selectIfNull(sum("a"), 0).from("t")
//        println(select.sql())

//        val select = Select().select(countDistinct("a")).from("t")
//        println(select.sql())

//        val select = Select(DB.MYSQL).select(cast(const(1234)))
//        println(select.sql())

//        val select = Select().select(stringAgg(column("a"), ",", DB.MYSQL) alias "a").from("t")
//        println(select.sql())

//        val select = Select(DB.PGSQL).selectStringAgg(column("a"), ",", column("a").orderByDesc(), true, "aaa").from("t")
//        println(select.sql())

//        val select = Select(DB.MYSQL)
//                .select(arrayAgg(column("a"), ",", DB.MYSQL, column("a").orderByAsc(), true))
//                .from("t")
//        println(select.sql())

//        val select = Select(DB.PGSQL)
//                .selectStringAgg(column("a"), ",", column("a").orderByAsc(), true)
//                .from("t")
//        println(select.sql())

//        val select = Select().from<User>()
//        println(select.sql())

//        val select = Select(DB.MYSQL)
//            .from("audit_rule")
//            .where(column("tables").json(0, DB.MYSQL).json("id").jsonText("name") eq "129")
//        println(select.sql())

//        val select = Select(DB.PGSQL).from("t").where(findInSet(column("c"), column("a"), DB.PGSQL) alias "aaa")
//        println(select.sql())

//        val select = Select().from("table")
//        println(select.sql())

//        val select = Select().from("t").select(concatWs(",", const("1"), column("a"), const("1")))
//        println(select.sql())

//        val select = Select(DB.PGSQL).from("t")
//            .select(jsonLength(column("tables").json(0, DB.PGSQL).json("arr"), DB.PGSQL))
//        println(select.sql())

//        val select = Select()
//                .from("table")
//                .select("a" alias "c1", "b" alias "c2")
//                .where(column("id") eq "1")
//                .orderByAsc("a")
//                .limit(10, 100)
//        println(select.sql())

//        val select = Select()
//                .from("table")
//                .select(count())
//                .groupBy("column")
//                .sql()
//        println(select)

//        val select = Select()
//                .from("table")
//                .select((column("a") + column("b")) / const(2) alias "col")
//                .where((column("a") eq 1) and (column("b") eq 2))
//                .sql()
//        println(select)

//        val select = Select()
//                .from("table")
//                .where(column("a") inList listOf(1, 2))
//                .where(column("b").isNotNull())
//                .where(column("c") like "%value%")
//                .sql()
//        println(select)

//        val select = Select()
//                .from(User)
//                .select(case(User.gender eq 1 then "男", User.gender eq 2 then "女") elseIs "其他" alias "gender")
//                .sql()
//        println(select)

//        val case = case(User.gender eq 1 then User.gender) elseIs null
//        val select = Select()
//                .from(User)
//                .select(count(case) alias "male_count")
//                .sql()
//        println(select)

//        val select = (Select().from("t1").select("a") union
//                Select().from("t2").select("a") unionAll
//                Select().from("t3").select("a")).sql()
//        println(select)

//        val select = Select()
//                .from("t1")
//                .leftJoin(Select().from("t2").limit(1), alias = "t2", on = column("t1.id") eq column("t2.id"))
//                .sql()
//        println(select)

//        val select = Select().from(Select().from("table")).sql()
//        println(select)

//        val select = Select()
//                .from("t1")
//                .select(column("id") inList Select().from("t2").select("id"))
//                .sql()
//        println(select)

//        val select = Select()
//                .from("table")
//                .select(concat(column("a"), const(","), column("b")))
//                .select(concatWs(",", column("a"), column("b")))
//                .sql()
//        println(select)

//        val select = Select()
//                .from("table")
//                .select(ifNull(sum("col"), const(0), DB.MYSQL))
//                .sql()
//        println(select)

//        val select = Select().from("table").select(cast(column("a"), "char")).sql()
//        println(select)

//        val select = Select()
//                .from("table")
//                .select(jsonLength(column("json_col").json(0, DB.MYSQL).json("objs"), DB.MYSQL))
//                .sql()
//        println(select)

//        val select = Select(DB.PGSQL).from("table").select(findInSet("1", column("a"), DB.PGSQL)).sql()
//        println(select)

//        val select = Select(DB.PGSQL)
//                .from("table")
//                .select(arrayAgg(column("a"), ",", DB.PGSQL, column("a").orderByAsc(), true))
//                .sql()
//        println(select)
//
//        val select = Select()
//                .from(User)
//                .select(User.name alias "c2", countDistinct(User.id) alias "count")
//                .where(User.id gt 1)
//                .groupBy(User.name)
//                .sql()
//        println(select)

//        val select = Select()
//                .from(User)
//                .select(User.id + 1)
//                .where((User.name like "%xxx%") or (User.name inList Select().from(User).select(User.name)) and (User.id gt 1))
//                .orderByAsc(User.id)
//                .sql()
//        println(select)

//        val select = (Select().from(User).select(User.id alias "id") union
//                Select().from(User).select(User.id alias "id")).sql()
//        println(select)

//        val select = Select().from(User).leftJoin(User1, on = User.id eq User1.id).sql()
//        println(select)

//        val select = Select(DB.ORACLE).from(User).select(findInSet("1", User.name)).sql()
//        println(select)

//        val select = Select(DB.PGSQL).from(User).select(ifNull(User.name, "")).sql()
//        println(select)

        val select = Select(DB.MYSQL).from(User).select(User.name.json("a").json(1).jsonText("b")).sql()
        println(select)
    }
}