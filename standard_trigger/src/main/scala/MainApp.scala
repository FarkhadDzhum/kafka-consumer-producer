import kz.dmc.packages.enums.DMCStatuses
import kz.dmc.packages.spark.DMCSpark
import Standard_Trigger.runTask


object MainApp {
  def main(args: Array[String]): Unit = {
    try {
      val spark = DMCSpark.get(MainApp.getClass)
        .setParams(args(0))
        .startWork()
        .getSparkSession()

      DMCSpark.get.computingLog(DMCStatuses.COMPUTING_INFO, "runTask начат")
      runTask(spark)
      DMCSpark.get.computingLog(DMCStatuses.COMPUTING_INFO, "runTask закончен")

      DMCSpark.get.finishWork()

    } catch {
      case e: Exception =>
        e.printStackTrace()
        DMCSpark.get().errorWork(e)
    }

  }
}