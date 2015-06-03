package eu.europeana.harvester.db.swift;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by salexandru on 03.06.2015.
 */
public class SwiftMediaStorageClientImplTest {
    private static final String containerName = "swiftUnitTesting";
    private static final AccountConfig accountConfig = new AccountConfig();

    static {
        accountConfig.setUsername("c9b9ddb5-4f64-4e08-9237-1d6848973ee1.swift.user@a9s.eu");
        accountConfig.setPassword("78ae7i9XO3O7CcdkDa87");
        accountConfig.setAuthUrl("https://auth.hydranodes.de:5000/v2.0");
        //accountConfig.setTenantId("c9b9ddb5-4f64-4e08-9237-1d6848973ee1.swift.user@a9s.eu");
        accountConfig.setAuthenticationMethod(AuthenticationMethod.BASIC);
    }

    private SwiftMediaStorageClientImpl mediaStorageClient;

    @Before
    public void setUp() {
        try {
            final AccountFactory factory = new AccountFactory(accountConfig);
            if (null == factory) {
                System.out.println("Wtf ??");
                return;
            }
            final Account account = factory.createAccount();

            if (null == account) {
                System.out.println("Wtf ??");
                return;
            }
            if (!account.getContainer(containerName).exists()) {
                account.getContainer(containerName).create();
            }
        }
        catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
//        new AccountFactory(accountConfig).createAccount().getContainer(containerName).delete();
    }

    @Test
    public void test() {

    }
}
