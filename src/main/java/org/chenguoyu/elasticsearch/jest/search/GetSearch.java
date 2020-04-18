package org.chenguoyu.elasticsearch.jest.search;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;
import org.chenguoyu.elasticsearch.jest.Employee;
import org.junit.Before;

import java.io.IOException;

public class GetSearch {
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

    public void getByDocumentId() throws IOException {
        DocumentResult employees = jestClient.execute(new Get.Builder("employees", "17").build());

        Employee getResult = jestClient.execute(new Get.Builder("employees", "1").build())
                .getSourceAsObject(Employee.class);
    }
}
