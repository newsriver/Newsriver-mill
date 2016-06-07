package ch.newsriver.mill;

import ch.newsriver.executable.Main;
import ch.newsriver.executable.poolExecution.MainWithPoolExecutorOptions;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Option;

/**
 * Created by eliapalme on 11/03/16.
 */
public class MillMain extends MainWithPoolExecutorOptions {

    private static final int DEFAUTL_PORT = 9098;
    private static final Logger logger = LogManager.getLogger(MillMain.class);


    public int getDefaultPort(){
        return DEFAUTL_PORT;
    }

    static Mill mill;

    public MillMain(String[] args){
        super(args,true);

    }

    public static void main(String[] args){
        new MillMain(args);
    }

    public void shutdown(){

        if(mill!=null)mill.stop();
    }

    public void start(){
        try {
            System.out.println("Threads pool size:" + this.getPoolSize() +"\tbatch size:"+this.getBatchSize()+"\tqueue size:"+this.getQueueSize());
            mill = new Mill(this.getPoolSize(),this.getBatchSize(),this.getQueueSize(),this.isPriority());
            new Thread(mill).start();
        } catch (Exception e) {
            logger.fatal("Unable to initialize scout", e);
        }
    }


}
