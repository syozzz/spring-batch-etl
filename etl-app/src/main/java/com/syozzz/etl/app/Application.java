package com.syozzz.etl.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(
        scanBasePackages = {"com.syozzz.etl"}
)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
