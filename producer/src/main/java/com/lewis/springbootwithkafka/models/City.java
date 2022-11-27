package com.lewis.springbootwithkafka.models;

import java.io.Serializable;

public class City  implements Serializable {

    private String name;
    private String UF;

    public City() {

    }

    public City(String name, String UF) {
        this.name = name;
        this.UF = UF;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUF() {
        return UF;
    }

    public void setUF(String UF) {
        this.UF = UF;
    }
}
