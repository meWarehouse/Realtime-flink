package com.at.bean

import scala.beans.BeanProperty

case class ItemViewCount(@BeanProperty itemId: Long, // 商品id
                         @BeanProperty windowEnd: Long, // 窗口结束时间
                        @BeanProperty count: Long) // itemId在windowEnd所属的窗口中被浏览的次数
