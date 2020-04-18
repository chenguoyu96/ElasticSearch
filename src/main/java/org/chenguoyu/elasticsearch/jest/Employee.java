package org.chenguoyu.elasticsearch.jest;

import lombok.Data;
import java.util.List;

@Data
public class Employee {
    private String name;
    private String title;
    private List<String> skills;
    private Integer yearsOfService;
}
