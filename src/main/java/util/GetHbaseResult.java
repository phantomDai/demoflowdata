package util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static util.ConfigurationHbase.TABLENAME;

public class GetHbaseResult {

    //the object is used to configurate Hbase
    private Configuration configurationHbase;

    //this object is a Communicator to hbase
    private Connection connectionHbase;







    /**
     * initialization hbase before operating files
     */
    private void initHbase(){
        // initialize configuration and connection
        configurationHbase = HBaseConfiguration.create();
        configurationHbase.set("hbase.zookeeper.quorum", "10.0.0.52");
        try {
            connectionHbase = ConnectionFactory.createConnection(configurationHbase);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }


    /**
     * get data from hbase
     * @param deviceId device identification
     * @return target info
     */
    public Map<String,Object> getTargetId(String deviceId){
        //get table
        HTable table = null;
        try {
            table = (HTable) connectionHbase.getTable(TableName.valueOf(TABLENAME));
        } catch (IOException e) {
            e.printStackTrace();
        }


        //initialization a scan object
        Scan scan = new Scan();

        //set the number of row
        scan.setMaxResultSize(1);

        //filter rows
        scan.setStartRow(Bytes.toBytes(deviceId));
        scan.setStopRow(Bytes.toBytes(deviceId));

        //keep the latest data at the top
        scan.setReversed(true);

        //record result
        Map<String,Object> resultMap = new HashMap<>();

        try {
            ResultScanner results = table.getScanner(scan);
            for (Result res : results) {
                List<Cell> cells = res.listCells();
                //get all columns
                for (Cell cell: cells) {
                    resultMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
                            Bytes.toString(CellUtil.cloneValue(cell)));
                }
                break;//only get thelatest data
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //remove the date of record_time
        resultMap.remove("record_time");

        return resultMap;
    }









}
