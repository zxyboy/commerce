object TestSalca {
    def main(args: Array[String]): Unit = {
        //**************  子类可以赋值给父类
        val xiaoMing: Chinese = new Chinese
        val asia: Asia = xiaoMing

        //**************  T 不变 ： 泛型类型不会发生改变
        // val groupChinese : Group[Chinese] = new Group[Chinese](new Chinese)
        //val groupAsia : Group[Asia] = groupChinese // 报错 : 类型不匹配


        //**************  +T 协变 ： 泛型类型能够向上转化
        //        val groupChinese : Group1[Asia] = new Group1[Asia](new Asia)
        //        val groupAsia : Group1[Chinese] = groupChinese // 报错类型不匹配
        // 说明： +T 类型是能够使用父类型的泛型接受的
        //        val groupChinese : Group1[Chinese] = new Group1[Chinese](new Chinese)
        //        val groupAsia : Group1[Asia] = groupChinese

        //**************  -T 逆变 ：泛型类型可以向下转化
        //        val groupAsia : Group2[Chinese] = new Group2[Chinese](new Chinese)
        //        val groupChinese : Group2[Asia] = groupAsia // 报错类型不匹配
        // 说明： -T 逆变是能够使用子类泛型接受的
        //        val groupAsia: Group2[Asia] = new Group2[Asia](new Asia)
        //        val groupChinese: Group2[Chinese] = groupAsia
        //**************  <: 上界 ： 说明Asia是T 接受的最大类型，即上界
        //        val groupPerson : Group3[Person] = new Group3[Person](new Person) //报错类型不匹配
        //        val groupChinese : Group3[Chinese] = new Group3[Chinese](new Chinese)

        //**************  >: 下界 ： 说明Asia是T接受的最小类型，即下界
        //        val groupPerson : Group4[Person] = new Group4[Person](new Person)
        //        val groupChinese : Group4[Chinese] = new Group4[Chinese](new Chinese)//报错类型不匹配


        //val asiaPair: Pair[Asia] = new Pair[Asia](new Asia, new Asia)
        //        asiaPair.replaceFirst[Person](new Person)
        //        asiaPair.replaceFirst[Chinese](new Chinese) //报错类型不匹配
        //
        //        asiaPair.replaceSecond[Person](new Person)//报错类型不匹配
        //          asiaPair.replaceSecond[Chinese](new Chinese)

        val unit = new Group5[Asia](new Asia)
        // unit.f3[Chinese](new Chinese) 报错
    }
}

//*************** 协变、逆变中的上下界 【里氏替换原则】
class Group5[+T](t : T){
    def f1[T](t : T) = {}
    //def f2[S <: T](s : S) = {}  协变不能使用上限，因为协变和上限相互冲突
    def f3[Z >: T](z : Z) = {}
}

class Group6[-T](t : T){
    def f1[T](t : T) = {}
    def f2[S <: T](s : S) = {}
    //def f3[Z >: T](z : Z) = {} 逆变不能使用下限
}
//*************** 方法中的上下界
class Pair[T](val first: T, val second: T) {
    def replaceFirst[R >: T](newFirst: R) = new Pair[R](newFirst, second)

    def replaceSecond[S <: T](newSecond: S) = new Pair[T](first, newSecond)
}

//**************  >: 下界 ： 说明Asia是T接受的最小类型，即下界
class Group4[T >: Asia](t: T)


//**************  <: 上界   ： 说明Asia是T 接受的最大类型，即上界
class Group3[T <: Asia](t: T)


// 逆变
class Group2[-T](t: T)


//协变
class Group1[+T](t: T)


//不变
class Group[T](t: T)


class Person

class Asia extends Person

class Chinese extends Asia



