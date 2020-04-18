package org.chenguoyu.elasticsearch.jest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.AddAliasMapping;
import io.searchbox.indices.aliases.ModifyAliases;
import io.searchbox.indices.aliases.RemoveAliasMapping;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 创建索引
 */

public class IndexManage {
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

    /**
     * 判断索引是否存在
     *
     * @throws IOException
     */
    @Test
    public void indexExist() throws IOException {
        JestResult result = jestClient.execute(new IndicesExists.Builder("employees").build());
        if (result.isSucceeded()) {
            System.out.println("employees索引存在");
        } else {
            System.out.println("employees索引不存在");
        }
    }

    /**
     * 创建索引
     *
     * @throws IOException
     */
    @Test
    public void createIndex() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        JestResult result = jestClient.execute(new CreateIndex.Builder("employees").settings(settings).build());
        if (result.isSucceeded()) {
            System.out.println("employees索引创建成功");
        } else {
            System.out.println("employees索引创建失败");
        }
    }

    @Test
    public void deleteIndex() throws IOException {
        JestResult result = jestClient.execute(new DeleteIndex.Builder("employees").build());
        if (result.isSucceeded()) {
            System.out.println("employees删除成功");
        } else {
            System.out.println("employees删除失败");
        }
    }

    /**
     * 给索引起别名
     */
    @Test
    public void addAlias() throws IOException {
        JestResult result = jestClient.execute(new ModifyAliases.Builder(new AddAliasMapping.Builder("employees", "e").build()).build());
        if (result.isSucceeded()) {
            System.out.println("employees的别名是e");
        } else {
            System.out.println(result.getErrorMessage());
        }
    }

    /**
     * 移除索引的别名
     */
    @Test
    public void removeAlias() throws IOException {
        JestResult result = jestClient.execute(new ModifyAliases.Builder(new RemoveAliasMapping.Builder("employees", "e").build()).build());
        if (result.isSucceeded()) {
            System.out.println("移除employees的别名e");
        } else {
            System.out.println(result.getErrorMessage());
        }
    }
}
