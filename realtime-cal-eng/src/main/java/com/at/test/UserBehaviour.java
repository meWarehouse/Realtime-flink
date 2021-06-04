package com.at.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zero
 * @create 2021-05-31 13:02
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBehaviour {

    Long userId;
    Long itemId;
    Integer categoryId;
    String behaviour;
    Long timestamp;

}
