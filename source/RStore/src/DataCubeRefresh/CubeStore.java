package DataCubeRefresh;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;


public class CubeStore {

    /* An entity class. */
    @Entity
    static class Cube {

        @PrimaryKey
        String cubeKey;        
        float nValue;
        

        Cube(String dKey, float nValue) {
            this.cubeKey = dKey;
            this.nValue = nValue;
        }

        private Cube() {} // For deserialization
    }
    
    
    static class CubeAccessor {

        /* Person accessors */
        PrimaryIndex<String,Cube> cubeByKey;


        /* Opens all primary and secondary indices. */
        public CubeAccessor(EntityStore store)
            throws DatabaseException {
        	cubeByKey = store.getPrimaryIndex(
                String.class, Cube.class);
        }
    }

    
    public Environment env;
    public EntityStore store;
    public CubeAccessor ca;
    public File dbHome;

    void init(){
    	EnvironmentConfig envConfig = new EnvironmentConfig();
    	envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        System.out.println(envConfig.getCachePercent());
        env = new Environment(dbHome, envConfig);
        
        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(false);
        store = new EntityStore(env, "CubeStore", storeConfig);        
        ca = new CubeAccessor(store);
    }
    
    public CubeStore(File path){
    	dbHome = path;
    	init();
    }
    
    public static void main(String args[]) throws IOException{
    	File path = new File("E:\\tmp");
    	if(!path.exists())
    		path.mkdirs();
    	//if(!path.exists())
    	//	path.createNewFile();
    	CubeStore kvStore = new CubeStore(path);
    	Random random = new Random();
    	int i = 0;
    	while(true){
    		i ++;
    		if(i % 100 == 0)
    			System.out.println(i);
    		int key = random.nextInt(8000000);
    		kvStore.ca.cubeByKey.put(new Cube("asldkfjasdkfjasdfasdfasdfasdfsdfasdf", 12341));
    	}

    }
    
    
    public void close()
            throws DatabaseException {
            store.close();
            env.close();
        }

	public void restart() throws IOException {		
		store.close();
		env.close();
		FileUtils.deleteDirectory(dbHome);
		if (!dbHome.exists())
			dbHome.mkdirs();
		init();				
	}
    
}
