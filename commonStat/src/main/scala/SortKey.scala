case class SortKey(clickCount: Long, orderCount: Long, payCount: Long) extends Ordered[SortKey] {

    override def compare(that: SortKey): Int = {
        val clickResult = java.lang.Long.compare(this.clickCount, that.clickCount)
        val result = if (clickResult == 0) {
            val orderResult = java.lang.Long.compare(this.orderCount, that.orderCount)
            if (orderResult == 0) {
                java.lang.Long.compare(this.payCount, that.payCount)
            } else {
                orderResult
            }
        } else {
            clickResult
        }
        result
    }
}
