package org.apache.cassandra.tools;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.silveregg.wrapper.WrapperListener;
import com.silveregg.wrapper.WrapperManager;


/**
 * Wrapper listener for cassandra server
 * 
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class WrapperMain implements WrapperListener
{
/*    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(WrapperMain.class);
    private CassandraDaemon daemon;

    
    /**
     * 
     */
    public WrapperMain(CassandraDaemon daemon)
    {
        this.daemon = daemon;
    }
    
    

    public static void main (String[] args)
    {
        try
        {
            WrapperManager.start(new WrapperMain(new CassandraDaemon()), args);
        } catch (Exception e)
        {
            e.printStackTrace();
            WrapperManager.stop(-1);
        }

    }

    public Integer start(final String[] args) 
    {
        WrapperManager.signalStarting(3600000);

        try {
            daemon.setup();
            WrapperManager.signalStarting(30000);
            
            new Thread(new Runnable()
            {
                
                @Override
                public void run()
                {
                    daemon.start();
                }
            },"ThriftMain").start();
            
        }
        catch (Throwable e)
        {
            String msg = "Exception encountered during startup.";
            logger.error(msg, e);

            // try to warn user on stdout too, if we haven't already detached
            System.out.println(msg);
            e.printStackTrace();

            System.exit(3);
        }
        
        return null;
    }
    
    public int stop(int exitCode)
    {
        WrapperManager.signalStopping(30000);
        daemon.stop();
        
        WrapperManager.signalStopping(300000);
        try {
            StorageService.instance.drain();
        } catch (Exception e) {
            logger.error("Drain failed",e);
        }
        
        return exitCode;
    }

    public void controlEvent(int event)
    {
        if (!WrapperManager.isControlledByNativeWrapper())
            WrapperManager.stop(0);
    }

}
