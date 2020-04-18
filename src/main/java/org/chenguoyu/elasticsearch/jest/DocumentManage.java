package org.chenguoyu.elasticsearch.jest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DocumentManage {
    private JestClient jestClient;

    @Before
    public void jestClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://127.0.0.1:9200/")
                        .multiThreaded(true)
                        .defaultMaxTotalConnectionPerRoute(2)
                        .maxTotalConnection(10)
                        .build());
        jestClient = factory.getObject();
    }

    @Test
    public void createDocument() throws IOException {
        Map<String, Object> employeeHashMap = new LinkedHashMap<>();
        employeeHashMap.put("name", "Michael Pratt");
        employeeHashMap.put("title", "Java Developer");
        employeeHashMap.put("yearsOfService", 2);
        employeeHashMap.put("skills", Arrays.asList("java", "spring", "elasticsearch"));
        Index employees = new Index.Builder(employeeHashMap)
                .index("employees")
                .build();
        jestClient.execute(employees);
    }

    @Test
    public void createDocumentByJackSon() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode employeeJsonNode = mapper.createObjectNode()
                .put("name", "Michael Pratt")
                .put("title", "Java Developer")
                .put("yearsOfService", 2)
                .set("skills", mapper.createArrayNode()
                        .add("java")
                        .add("spring")
                        .add("elasticsearch"));
        Index employees = new Index.Builder(employeeJsonNode.toString())
                .index("employees")
                .type("employee")
                .build();
        DocumentResult execute = jestClient.execute(employees);
        System.out.println(execute.isSucceeded());
    }

    @Test
    public void createDocumentByPoJo() throws IOException {
        Employee employee = new Employee();
        employee.setName("Michael Pratt");
        employee.setTitle("Java Developer");
        employee.setYearsOfService(2);
        employee.setSkills(Arrays.asList("java", "spring", "elasticsearch"));
        Index employees = new Index.Builder(employee)
                .index("employees")
                .type("employee")
                .build();
        jestClient.execute(employees);
    }

    @Test
    public void updateDocument() throws IOException {
        Employee employee = new Employee();
        employee.setYearsOfService(3);
        jestClient.execute(new Update.Builder(employee).index("employees").id("1").build());
    }

    @Test
    public void deleteDocument() throws IOException {
        jestClient.execute(new Delete.Builder("17").index("employees").build());
    }

    @Test
    public void batchInsert() throws IOException {
        Employee employee1 = new Employee();
        employee1.setName("Michael Pratt");
        employee1.setTitle("Java Developer");
        employee1.setYearsOfService(2);
        employee1.setSkills(Arrays.asList("java", "spring", "elasticsearch"));
        Employee employee2 = new Employee();
        employee2.setName("Michael Pratt");
        employee2.setTitle("Java Developer");
        employee2.setYearsOfService(2);
        employee2.setSkills(Arrays.asList("java", "spring", "elasticsearch"));

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("employees")
                .defaultType("employee")
                .addAction(new Index.Builder(employee1).build())
                .addAction(new Index.Builder(employee2).build())
//                .addAction(new Delete.Builder("17").build())
                .build();
        BulkResult execute = jestClient.execute(bulk);
        if (execute.isSucceeded()) {
            System.out.println("插入成功");
        } else {
            System.out.println(execute.getErrorMessage());
        }
    }

    @Test
    public void addAsync() {
        Employee employee = new Employee();
        employee.setName("Michael Pratt");
        employee.setTitle("Java Developer");
        employee.setYearsOfService(2);
        employee.setSkills(Arrays.asList("java", "spring", "elasticsearch"));
        jestClient.executeAsync(
                new Index.Builder(employee).index("employees").type("employee").build(),
                new JestResultHandler<JestResult>() {
                    @Override
                    public void completed(JestResult result) {
                        if (result.isSucceeded()) {
                            System.out.println("插入成功");
                        } else {
                            System.out.println(result.getErrorMessage());
                        }
                    }

                    @Override
                    public void failed(Exception ex) {
                        System.out.println(ex.getMessage());
                    }
                });
        try {
            TimeUnit.SECONDS.sleep(5L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
