package com.at.bean

import scala.beans.BeanProperty

/**
 * @author zero
 * @create 2021-06-25 22:07
 */

//543462,1715,1464116,pv,1511658000
//@Data
//case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behaviour: String, timestamp: Long)


case class UserBehavior(@BeanProperty userId: Long,
                        @BeanProperty itemId: Long,
                        @BeanProperty categoryId: Int,
                        @BeanProperty behaviour: String,
                        @BeanProperty timestamp: Long)
