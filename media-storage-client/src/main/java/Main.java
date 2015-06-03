import com.google.common.collect.ImmutableSet;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.jclouds.ContextBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.google.inject.Module;
import org.jclouds.io.Payload;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.domain.Container;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;
import org.jclouds.openstack.swift.v1.options.CreateContainerOptions;
import org.jclouds.openstack.swift.v1.options.PutOptions;

/**
 * Created by salexandru on 03.06.2015.
 */
public class Main {
    public static void main(String[] args) {
        try {
            final AccountConfig accountConfig = new AccountConfig();
            accountConfig.setUsername("c9b9ddb5-4f64-4e08-9237-1d6848973ee1.swift.user@a9s.eu");
            accountConfig.setPassword("78ae7i9XO3O7CcdkDa87");
            accountConfig.setAuthUrl("https://auth.hydranodes.de:5000/v2.0");
            accountConfig.setTenantId("3c678adbb69641018b645caa104b9252");
            new AccountFactory(accountConfig).createAccount();
        }
        finally {

        }
       // catch (Exception e) {
         //   System.out.println(e.getMessage());
       // }

        try {
            SwiftApi siwftApi;
            Iterable<Module> modules = ImmutableSet.<Module>of(new SLF4JLoggingModule());
            String provider = "openstack-nova";
            String identity = "c9b9ddb5-4f64-4e08-9237-1d6848973ee1.swift.user@a9s.eu"; // tenantName:userName
            String credential = "78ae7i9XO3O7CcdkDa87";

            siwftApi = ContextBuilder.newBuilder(provider)
                                    .endpoint("https://auth.hydranodes.de:5000/v2.0")
                                    .credentials(identity, credential)
                                    .modules(modules)
                                    .buildApi(SwiftApi.class);
            siwftApi.getContainerApi("RegionOne").create("unittest_Alex");

            System.out.println("working ?");
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
