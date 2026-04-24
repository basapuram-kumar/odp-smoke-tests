// Loaded by spark-sample-smoke.sh via spark-shell -i (sc is pre-defined).

val simpleSum = 1 + 1
println(simpleSum)

val data = Seq(1, 2, 3, 4, 5)
val rdd = sc.parallelize(data)
println(rdd.collect().mkString("[", ", ", "]"))

val rddSquared = rdd.map(x => x * x)
println(rddSquared.collect().mkString("[", ", ", "]"))

val evenNumbers = rdd.filter(_ % 2 == 0)
println(evenNumbers.collect().mkString("[", ", ", "]"))

val reducedSum = rdd.reduce(_ + _)
println(reducedSum)

sys.exit(0)
