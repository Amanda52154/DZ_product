package com.ZJS.demo;

import com.JLC.demo.ApiHelper;
import com.JLC.demo.JLCAllData2Tidb;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * DzProduce   com.ZJS.demo
 * 2023-03-2023/3/25   12:13
 *
 * @author : zhangmingyue
 * @description : ZJS Unified Process
 * @date : 2023/3/21 12:13 PM
 */


public class ZJSUnifiedFormat extends ApiHelper {
    public ZJSUnifiedFormat(String apiUrl) {
        super(apiUrl);
    }

    public static void main(String[] args) throws IOException {

        String appName = "ZJSUnifiedFormat";
        SparkSession sparkSession = defaultSparkSession(appName);

        String indicatormainTable = "c_in_indicatormain";
        String systemconstTable = "c_in_systemconst";
        String sinkTable = "st_c_in_indicatormain";

        getDF(sparkSession, indicatormainTable).createOrReplaceTempView("indicatormain");
        getDF(sparkSession, systemconstTable).createOrReplaceTempView("systemconst");

        Dataset<Row> tidbDF = sparkSession.sql( getSql());
        tidbDF.show();
//        writeToTiDB(tidbDF, sinkTable);
        sparkSession.stop();

    }
    private static String getSql(){
        return "select ID,\n" +
                "       IndicatorCode,\n" +
                "       IndicatorName,\n" +
                "       InfoSourceCode,\n" +
                "       InfoSource,\n" +
                "       PowerNumber,\n" +
                "       IndiState,\n" +
                "       BeginDate,\n" +
                "       EndDate,\n" +
                "       UnitCode,\n" +
                "       UnitCode as UnitCodeReport,\n" +
                "       case\n" +
                "           when tmp1.DisclosureFrequency = '1201' then 'DAY'\n" +
                "           when tmp1.DisclosureFrequency = '1202' then 'WEEK'\n" +
                "           when tmp1.DisclosureFrequency = '1203' then 'MONTH'\n" +
                "           when tmp1.DisclosureFrequency = '1204' then 'QUARTER'\n" +
                "           when tmp1.DisclosureFrequency = '1205' then 'HALF YEAR'\n" +
                "           when tmp1.DisclosureFrequency = '1206' then 'YEAR'\n" +
                "           when tmp1.DisclosureFrequency = '1207' then 'TEN DAYS'\n" +
                "           when tmp1.DisclosureFrequency = '1208' then 'HALF MONTH'\n" +
                "           when tmp1.DisclosureFrequency = '1209' then 'ANY'\n" +
                "       end as DisclosureFrequency,\n" +
                "       IndiRemark,\n" +
                "       UpdateTime,\n" +
                "       JSID,\n" +
                "       zjs_insert_time,\n" +
                "       zjs_update_time\n" +
                "from\n" +
                "(select ID,\n" +
                "       IndicatorCode,\n" +
                "       IndicatorName,\n" +
                "       InfoSourceCode,\n" +
                "       InfoSource,\n" +
                "       PowerNumber,\n" +
                "       IndiState,\n" +
                "       BeginDate,\n" +
                "       EndDate,\n" +
                "       case\n" +
                "           when sys.DM in ('170095','170099','170234','170310','170368','170825','171028','171219','170051') then '元'\n" +
                "           when sys.DM in ('170003','170028','170066','170188','170211','170237','170238','170272','170363','170408','170437','170438','170439','170441','170470','170472','170507','170508','170541','170542','170543','170068','170804','170871','171077','171196') then '千克'\n" +
                "           when sys.DM in ('170002','170007','170351','170353','170402','170557','170558','170484','170869','171054','171218') then '万吨'\n" +
                "           when sys.DM in ('170040','170042','170082','170493','170600','170789'',''170803','171108') then '升'\n" +
                "           when sys.DM in ('170037','170112','170196','170197','170903','171026') then '只'\n" +
                "           when sys.DM in ('170038','170044','170401','170796','171111') then '头'\n" +
                "           when sys.DM in ('170039','170043','170045','170075','170111','170117','170176','170177','170198','170207','170208','170218','170224','170241','170289','170290','170294','170319','170320','170384','170404','170411','170496','170497','170623','170634','170813','170847','170862','170938','170939','171012','171298','171309') then '个'\n" +
                "           when sys.DM in ('170041','170046','170178','170248','170219','170249','170255','171150') then '条'\n" +
                "           when sys.DM in ('170192','170609','171174') then '手'\n" +
                "           when sys.DM in ('170193','170203','170215','170369','170894','170896') then '张'\n" +
                "           when sys.DM in ('170070','170071','170072','170076','170078','170101','170102','170158','170184','170199','170200','170226','170245','170274','170276','170409','170551','170567','170582','170590','170758','170815','171119','171145','171300','171299') then '台'\n" +
                "           when sys.DM in ('170052','170164','170275','170374','170379','170670','170885','170918','171312') then '千瓦'\n" +
                "           when sys.DM in ('170054','170106','170279','170375','170622','170632','170651','170920','171008','171073') then '千瓦时'\n" +
                "           when sys.DM in ('170056','170059','170397','170468','170471','170473','170959') then '立方米'\n" +
                "           when sys.DM ='170067' then '蒲式耳'\n" +
                "           when sys.DM in ('170405','170407','170483','170521','170907','171304') then '包'\n" +
                "           when sys.DM in ('170107','170109','170114','170901') then '套'\n" +
                "           when sys.DM in ('170083','170476','171023','171306') then '支'\n" +
                "           when sys.DM in ('170089','170183','170229','170273','170352','170354','170357','170447','170475','170516','170817') then '人'\n" +
                "           when sys.DM ='171140' then '艘'\n" +
                "           when sys.DM = '170092' then '吨标准煤'\n" +
                "           when sys.DM in ('170295','170308','170574','170627','170628','170636','170912') then '次'\n" +
                "           when sys.DM = '170097' then '载重吨'\n" +
                "           when sys.DM = '170103' then '综合吨'\n" +
                "           when sys.DM in ('170110','170621') then '双'\n" +
                "           when sys.DM in ('170049','170055','170060','170105','170223','170271','170635','170641','170759','170761','170762','170765','170768','170771','170884','171006','171137','171220') then '平方米'\n" +
                "           when sys.DM in ('170155','170159','170162','170217','170752','170832','170909','170942','171171') then '焦'\n" +
                "           when sys.DM in ('170166','170168') then '度'\n" +
                "           when sys.DM ='170228' then '小时'\n" +
                "           when sys.DM in ('170205','170571','170924','170970') then '千伏安'\n" +
                "           when sys.DM in ('170057','170064','170108','170113','170161','170180','170356','170772','170818','171014','171092','171093','171130') then '千米'\n" +
                "           when sys.DM in ('170189','170212','170307','170311','170312','170630') then '家'\n" +
                "           when sys.DM in ('170410','171076','171301') then '付'\n" +
                "           when sys.DM in ('170175','170563') then '重量箱'\n" +
                "           when sys.DM in ('170181','170186','170281','170649','170819') then '吨公里'\n" +
                "           when sys.DM in ('170182','170185','170280') then '人公里'\n" +
                "           when sys.DM in ('170191','170440','170446','171116') then '股'\n" +
                "           when sys.DM in ('170195','171144') then '伏安时'\n" +
                "           when sys.DM = '170204' then '对开色令'\n" +
                "           when sys.DM = '170573' then '路端'\n" +
                "           when sys.DM ='171114' then '本'\n" +
                "           when sys.DM in ('170220','170309','170836') then '分钟'\n" +
                "           when sys.DM in ('170798','170834','17114') then 'GB'\n" +
                "           when sys.DM = '170250' then '信道'\n" +
                "           when sys.DM = '170222' then '门'\n" +
                "           when sys.DM = '170283' then '客位'\n" +
                "           when sys.DM = '171324' then '天'\n" +
                "           when sys.DM ='170559' then '人天'\n" +
                "           when sys.DM in ('170346','170349','170362') then '标准箱'\n" +
                "           when sys.DM in ('170347','170348') then 'DWT'\n" +
                "           when sys.DM in ('170350','170355','170650') then '吨海里'\n" +
                "           when sys.DM in ('170358','171117') then '位'\n" +
                "           when sys.DM in ('170359','171167') then '万座位公里'\n" +
                "           when sys.DM in ('170366','170367') then '笔'\n" +
                "           when sys.DM in ('170642','170879','171134') then '安时'\n" +
                "           when sys.DM ='170477' then '瓶'\n" +
                "           when sys.DM ='171047'  then '锭'\n" +
                "           when sys.DM ='171102' then '间'\n" +
                "           when sys.DM in ('170518','170893') then '盒'\n" +
                "           when sys.DM in ('170519','170991','171038','171183') then '粒'\n" +
                "           when sys.DM in ('170944','170985') then '单位'\n" +
                "           when sys.DM in ('171126','171127') then '亩'\n" +
                "           when sys.DM in ('170873','171034') then '箱'\n" +
                "           when sys.DM ='171305'\t then '金属吨'\n" +
                "           when sys.DM ='170620'\t then '吨位'\n" +
                "           when sys.DM ='170584'\t then 'SDR'\n" +
                "           when sys.DM ='170391' then '千吨油当量'\n" +
                "           when sys.DM in ('170390','170469','170552','170445') then '桶'\n" +
                "           when sys.DM ='170654' then '项'\n" +
                "           when sys.DM ='170801' then '修正总吨'\n" +
                "           when sys.DM ='170829' then '具'\n" +
                "           when sys.DM ='170892' then '万册'\n" +
                "           when sys.DM ='171322' then '名'\n" +
                "           when sys.DM in ('170902','171025') then '颗'\n" +
                "           when sys.DM ='170652' then '蒸吨'\n" +
                "           when sys.DM ='170937' then '码'\n" +
                "           when sys.DM ='171138' then '对'\n" +
                "           when sys.DM ='170997' then '打'\n" +
                "           when sys.DM ='170934' then '车次'\n" +
                "           when sys.DM in ('171007','171031') then '标方'\n" +
                "           when sys.DM in ('170754','171142') then 'TEU'\n" +
                "           when sys.DM in ('171027','171110') then '羽'\n" +
                "           when sys.DM in ('170495','171151','171152') then 'Bps'\n" +
                "           when sys.DM ='170934' then '车次'\n" +
                "           when sys.DM ='171115' then '单'\n" +
                "           when sys.DM ='170644'\t then '金衡盎司'\n" +
                "           when sys.DM ='171308' then '百万IU'\n" +
                "           else sys.MS\n" +
                "           end as UnitCode,\n" +
                "       UnitCodeReport,\n" +
                "       DisclosureFrequency,\n" +
                "       IndiRemark,\n" +
                "       UpdateTime,\n" +
                "       JSID,\n" +
                "       zjs_insert_time,\n" +
                "       zjs_update_time\n" +
                "from indicatormain main\n" +
                "         left join\n" +
                "         (select DM, MS from systemconst where LB=17) sys on main.UnitCode = sys.DM) as tmp1\n" +
                "    left join\n" +
                "    (select DM, MS from systemconst where LB=12) sys1 on tmp1.UnitCode = sys1.DM ";
    }
}
