package com.Test.demo;

/**
 * DZ_product   com.ZJS.demo
 * 2023-04-2023/4/20   14:49
 *
 * @author : zhangmingyue
 * @description :
 * @date : 2023/4/20 2:49 PM
 */

import java.util.Random;

public class RandomText {

    public static void main(String[] args) {
        System.out.println(generateRandomString(10));
    }

    public static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder stringBuilder = new StringBuilder(length);
        Random random = new Random();

        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(characters.length());
            stringBuilder.append(characters.charAt(randomIndex));
        }

        return stringBuilder.toString();
    }
}

