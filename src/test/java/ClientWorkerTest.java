import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LinkStore;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.LogStore;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientWorkerTest {

    private static final String TEST_VIEW = "test-view";
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_ENDPOINT = "cn-hangzhou-devcommon-intranet.sls.aliyuncs.com";
    private static final String TEST_LOGSTORE = "*";
    private static final String TEST_CONSUMER_GROUP_NAME = "consumer_group_client_worer_test";
    private static final String ACCESS_KEY_ID = "xxx";
    private static final String ACCESS_KEY = "xxx";

    public static void main(String[] args) throws LogHubClientWorkerException {

        final Client client = new Client(TEST_ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY);

        System.out.println("create project " + TEST_PROJECT);
        createProject(client, TEST_PROJECT);
        System.out.println("create view project" + TEST_VIEW);
        createProject(client, TEST_VIEW);
        // create logstore
        System.out.println("create logstore test-logstore-0");
        createLogStore(client, TEST_PROJECT, "test-logstore-0", 4);
        System.out.println("create logstore test-logstore-1");
        createLogStore(client, TEST_PROJECT, "test-logstore-1", 4);

        System.out.println("wait for create logstore finish");
        waitFor(120 * 1000);

        System.out.println("create link test-link-0");
        createLink(client, TEST_VIEW, "test-link-0", TEST_PROJECT, "test-logstore-0");
        System.out.println("create link test-link-1");
        createLink(client, TEST_VIEW, "test-link-1", TEST_PROJECT, "test-logstore-1");

        System.out.println("wait for create link finish");
        waitFor(120 * 1000);

        final ExecutorService writeLogExecutor = Executors.newFixedThreadPool(2);
        writeLogExecutor.submit(new Runnable() {
            @Override
            public void run() {
                writeLog(client, TEST_PROJECT, "test-logstore-0");
            }
        });
        writeLogExecutor.submit(new Runnable() {
            @Override
            public void run() {
                writeLog(client, TEST_PROJECT, "test-logstore-1");
            }
        });

        waitFor(10000);
        System.out.println("start consume...");

        int n = 5;
        Thread[] threads = new Thread[n];
        ClientWorker[] workers = new ClientWorker[n];

        for (int i = 0; i < n; i++) {
//            LogHubConfig config = new LogHubConfig(
//                    "consumer_group_client_worker_test", "consumer_" + i,
//                    TEST_ENDPOINT, TEST_PROJECT, TEST_LOGSTORE,
//                    ACCESS_KEY_ID, ACCESS_KEY,
//                    LogHubCursorPosition.BEGIN_CURSOR, 20 * 1000, false);
            LogHubConfig config = new LogHubConfig(TEST_CONSUMER_GROUP_NAME,
                                                   "consumer_" + i,
                                                   TEST_ENDPOINT,
                                                   TEST_VIEW,
                                                   TEST_LOGSTORE,
                                                   ACCESS_KEY_ID,
                                                   ACCESS_KEY,
                                                   LogHubConfig.ConsumePosition.BEGIN_CURSOR);
            ClientWorker worker = new ClientWorker(new LogHubProcessorTestFactory(), config);
            threads[i] = new Thread(worker);
            workers[i] = worker;
        }
        for (int i = 0; i < n; i++) {
            threads[i].start();
        }

        // sleep forever
        while(true)
        {
            waitFor(60 * 1000);
        }
    }

    private static void createLink(Client client, String testView, String s, String testProject, String s1) {
        LinkStore linkStore = new LinkStore(s, testProject, s1);
        try {
            client.CreateLinkStore(testView, linkStore);
        } catch (LogException e) {
            e.printStackTrace();
        }
    }

    private static void createProject(Client client, String testProject) {
        try {
            client.CreateProject(testProject, "test for view consumer group sdk");
        } catch (LogException e) {
            e.printStackTrace();
        }
    }

    private static void createLogStore(Client client, String testProject, String s, int i) {
        LogStore logStore = new LogStore();
        logStore.SetShardCount(i);
        logStore.SetTtl(3);
        logStore.SetLogStoreName(s);
        try {
            client.CreateLogStore(testProject, logStore);
        } catch (LogException e) {
            e.printStackTrace();
        }
    }

    /**
     * write sequence number one log per second
     *
     * @param projectName
     * @param logstoreName
     */
    private static void writeLog(Client client, final String projectName, final String logstoreName) {
        int i = 0;
        while (true) {
            try {
                List<LogItem> group = new ArrayList<LogItem>();
                LogItem item = new LogItem();
                item.PushBack("content", String.valueOf(i));
                group.add(item);
                client.PutLogs(projectName, logstoreName, "", group, "127.0.0.1");
                ++i;
                waitFor(500);
            } catch (LogException e) {
                e.printStackTrace();
            }
        }
    }

    private static void waitFor(int ms)
    {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
