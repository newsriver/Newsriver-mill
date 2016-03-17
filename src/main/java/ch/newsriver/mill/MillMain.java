package ch.newsriver.mill;

import ch.newsriver.executable.Main;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Option;

/**
 * Created by eliapalme on 11/03/16.
 */
public class MillMain extends Main {

    private static final int DEFAUTL_PORT = 9098;
    private static final Logger logger = LogManager.getLogger(MillMain.class);


    public int getDefaultPort(){
        return DEFAUTL_PORT;
    }

    static Mill mill;

    public MillMain(String[] args, Options options ){
        super(args,options);


    }

    public static void main(String[] args){

        Options options = new Options();

        options.addOption("f","pidfile", true, "pid file location");
        options.addOption(org.apache.commons.cli.Option.builder("p").longOpt("port").hasArg().type(Number.class).desc("port number").build());

        new MillMain(args,options);

    }

    public void shutdown(){

        if(mill!=null)mill.stop();
    }

    public void start(){
        try {
            mill = new Mill();
            new Thread(mill).start();
        } catch (Exception e) {
            logger.fatal("Unable to initialize scout", e);
        }
    }


}
