import com.alibaba.druid.sql.visitor.functions.IfNull
import dsl.*
import expr.DB
import select.Select

object TestSql {
    @JvmStatic
    fun main(args: Array<String>) {
//        val select = Select().from(User).sql()
//        println(select)

//        val select = Select(DB.MYSQL).from(User).select(User.id, User.name alias "name").sql()
//        println(select)

//        测试sql注入
//        val select = Select().from(User).select(User.id).where(User.name eq "a or 1 = 1").sql()
//        println(select)

//        val select = Select()
//            .from(User)
//            .select(User.id alias "c1", User.name alias "c2")
//            .where(User.id eq 1)
//            .orderByAsc(User.name)
//            .limit(10, 100)
//            .sql()
//        println(select)

//        val select = Select()
//            .from(User)
//            .select(User.name, count())
//            .groupBy(User.name)
//            .sql()
//        println(select)
//
//        val select = Select()
//            .from(User)
//            .select((User.id + User.gender) / 2 alias "col")
//            .where((User.id eq 1) and (User.gender eq 2))
//            .sql()
//        println(select)

//        val select = Select()
//                .from(User)
//                .where(User.gender inList listOf(1, 2))
//                .where((User.id notBetween 1 and 10) and (User.name eq ""))
//                .where(User.name.isNotNull())
//                .where(User.name like "%xxx%")
//                .sql()
//        println(select)

//        val userName: String? = null // 假如此处为用户传参
//        val select = Select()
//            .from(User)
//            .where({ !userName.isNullOrEmpty() }, User.name eq userName)
//            .sql()
//        println(select)

//        val select = Select()
//            .from(User)
//            .leftJoin(User1,on = User.id eq User1.id)
//            .sql()
//        println(select)

//        val select = Select()
//            .from(User)
//            .select(case(User.gender eq 1 then "男", User.gender eq 2 then "女") elseIs "其他" alias "gender")
//            .sql()
//        println(select)

//        val case = case(User.gender eq 1 then User.gender) elseIs null
//        val select = Select()
//            .from(User)
//            .select(count(case) alias "male_count")
//            .sql()
//        println(select)

//        val select = (Select().from(User).select(User.id) union
//                Select().from(User).select(User.id) unionAll
//                Select().from(User).select(User.id)).sql()
//        println(select)

//        val select = Select().from(Select().from(User)).sql()
//        println(select)

//        val select = Select()
//            .from(User)
//            .leftJoin(Select().from(User1).limit(1), on = column("t1.id") eq column("t2.id"))
//            .sql()

//        val select = Select()
//            .from(User)
//            .select(User.id inList Select().from(User).select(User.id).limit(10))
//            .sql()
//        println(select)

//        val select = Select()
//            .from(User)
//            .select(concat(User.id, const(","), User.name))
//            .select(concatWs(",", User.id, User.name))
//            .sql()
//        println(select)

//        val select = Select()
//            .from(User)
//            .select(ifNull(sum(User.age), 0))
//            .sql()
//        println(select)

//        val select = Select().from(User).select(cast(User.id, "CHAR")).sql()
//        println(select)

//        val select = Select(DB.PGSQL)
//            .from(User)
//            .select(User.jsonInfo json 0 json "obj" jsonText "id")
//            .sql()
//        println(select)

//        val select = Select(DB.PGSQL)
//            .from(User)
//            .select(stringAgg(User.name, ",", orderByAsc(User.id).orderByDesc(User.gender), true))
//            .sql()
//        println(select)

//        val select = Select(DB.PGSQL)
//            .from(User)
//            .select(jsonLength(User.jsonInfo.json(0).json("objs")))
//            .sql()
//        println(select)

//        val select = Select(DB.PGSQL).from(User).where(findInSet("1", User.ids)).sql()
//        println(select)

//        val select = Select()(listOf(User.id, count())) from
//                User alias "u" where ((User.id eq 1) and (User.name like "%xxx%")) groupBy
//                User.id orderByAsc User.id limit 10 offset 10
//        println(select.sql())

//        val select = Select() from User leftJoin
//                (Select() from User1) alias "a" on
//                (User.id eq column("a.id"))
//        println(select.sql())

//        val select = Select() select listOf(User.id alias "c1", User.name alias "c2") from
//                User where (User.id eq 1) orderByAsc User.name limit 10 offset 100
//        println(select.sql())
    }
}