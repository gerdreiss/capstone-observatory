package observatory

object Profiler {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val delta = t1 - t0
    val deltams = delta / (1000*1000)
    val ns = delta - deltams * 1000 * 1000
    println("Elapsed time: " + deltams + " ms plus " + ns + " ns")
    result
  }
}
