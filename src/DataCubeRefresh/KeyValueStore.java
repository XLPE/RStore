package DataCubeRefresh;
import java.io.File;
import java.io.IOException;
import java.util.Random;


import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;


public class KeyValueStore {

    /* An entity class. */
    @Entity
    static class KeyValue {

        @PrimaryKey
        int tupleKey;

        String dKey;
        float nValue;
        

        KeyValue(int partKey,String dKey, float nValue) {
            this.tupleKey = partKey;
            this.dKey = dKey;
            this.nValue = nValue;
        }

        private KeyValue() {} // For deserialization
    }
    
    
    static class PartAccessor {

        /* Person accessors */
        PrimaryIndex<Integer,KeyValue> partByKey;


        /* Opens all primary and secondary indices. */
        public PartAccessor(EntityStore store)
            throws DatabaseException {

        	partByKey = store.getPrimaryIndex(
                Integer.class, KeyValue.class);
        }
    }

    
    public Environment env;
    public EntityStore store;
    public PartAccessor pa;

    
    public KeyValueStore(File path){
    	EnvironmentConfig envConfig = new EnvironmentConfig();
    	envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        System.out.println(envConfig.getCachePercent());
        env = new Environment(path, envConfig);
        
        StoreConfig storeConfig = new StoreConfig();
        storeConfig.setAllowCreate(true);
        storeConfig.setTransactional(false);
        store = new EntityStore(env, "PartStore", storeConfig);
        
        pa = new PartAccessor(store);
    }
    
    public static void main(String args[]) throws IOException{
    	File path = new File("E:\\tmp");
    	if(!path.exists())
    		path.mkdirs();
    	//if(!path.exists())
    	//	path.createNewFile();
    	KeyValueStore kvStore = new KeyValueStore(path);
    	Random random = new Random();
    	kvStore.pa.partByKey.put(new KeyValue(1, "asdf", 12341));
    	kvStore.pa.partByKey.put(new KeyValue(2, "asdf", 12341));
    	KeyValue p = kvStore.pa.partByKey.get(1);
    	if(p == null){
    		System.out.println("null");
    	}
    	else{
    		System.out.println("not null");
    	}
    	
    	//kvStore.pa.partByKey.put(new Part(1, "1"));
    	//kvStore.pa.partByKey.put(new Part(2, "2"));
    	//kvStore.pa.partByKey.put(new Part(3, "3"));
    	
    	//System.out.println(kvStore.pa.partByKey.get(3).value);

    }
    
    
    public void close()
            throws DatabaseException {
            store.close();
            env.close();
        }
    
}
