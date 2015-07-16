import org.apache.hadoop.mapred.iterative.LoopReduceCacheFilter;
import org.apache.hadoop.io.*;

public class SSSPReduceCacheFilter implements LoopReduceCacheFilter {
		
		/*
        @Override
        public boolean isCache(Object key, Object value, int count) {
                
                if (count <= 1)
                        return false;
                else
                        return true;
                
                //if (value)
        }
        */
        
        @Override
        public boolean isCache(Object key, Object value, int count) {
        	Text valueText = (Text)value;
        	
        	if (valueText.toString().endsWith("COLOR")){
        		return false;
        	}
        	else {
        		return true;
        	}
        }

}
