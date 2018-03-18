package saveOffset.factory;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import saveOffset.enums.S0Enum;
import saveOffset.enums.S1Enum;
import saveOffset.model.NginxLogModel;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ModelFactory
{
  public static Logger log = Logger.getLogger(ModelFactory.class);

  public static NginxLogModel getModel(String nginxLog)
  {
    String[] nginxLogSplit = nginxLog.split(" ", -1);
    String bizType = nginxLogSplit[0];

    if ("s0".equalsIgnoreCase(bizType)) {
      String patternStr = "(.*?) (.*?) - (.*?) \\[(.*?)\\] \"(.*?)\" (.*?) (.*?) \"(.*?)\" \"(.*?)\" (.*?) (.*?) (.*?)$";
      List<String> dataList = getDataList(nginxLog,patternStr);

      if (dataList.size() == 0) {
        log.warn("datalist size = 0, bizType=" + bizType + ",nginxlog=" + nginxLog);
        return null;
      }

      String url = getUrl(dataList.get(S0Enum.URL.ordinal()));

      if (StringUtils.isEmpty(url) || url.contains(".")) {
        log.warn("url is empty, bizType=" + bizType + ",nginxlog=" + nginxLog);
        return null;
      }

      String minute = getMinute(dataList.get(S0Enum.TIME.ordinal()));

      if (StringUtils.isEmpty(minute)) {
        log.warn("minute is empty, bizType=" + bizType + ",nginxlog=" + nginxLog);
        return null;
      }

      Double upstreamResponseTime = 0d;

      try {
        upstreamResponseTime = Double.parseDouble(dataList.get(S0Enum.UPSTREAM_RESPONSE_TIME.ordinal()));
      } catch (Exception e) {
        upstreamResponseTime = -1d;
      }

      Double d = upstreamResponseTime * 1000;

      NginxLogModel model = new NginxLogModel();

      model.setBizType(bizType);
      model.setIp(getIp(dataList.get(S0Enum.IPS.ordinal())));
      model.setMinute(minute);
      model.setUrl(url);
      model.setStatus(dataList.get(S0Enum.STATUS.ordinal()));
      model.setUpstreamResponseTime(d.intValue());

      return model;
    } else if ("s1".equalsIgnoreCase(bizType)) {
      String patternStr = "(.*?) (.*?) - (.*?) \\[(.*?)\\] \"(.*?)\" (.*?) (.*?) \"(.*?)\" \"(.*?)\" (.*?) (.*?) (.*?)$";
      List<String> dataList = getDataList(nginxLog,patternStr);

      if (dataList.size() == 0) {
        log.warn("datalist size = 0, bizType=" + bizType + ",nginxlog=" + nginxLog);
        return null;
      }

      String url = getUrl(dataList.get(S1Enum.URL.ordinal()));

      if (StringUtils.isEmpty(url) || url.contains(".")) {
        log.warn("url is empty, bizType=" + bizType + ",nginxlog=" + nginxLog);
        return null;
      }

      String minute = getMinute(dataList.get(S1Enum.TIME.ordinal()));

      if (StringUtils.isEmpty(minute)) {
        log.warn("minute is empty, bizType=" + bizType + ",nginxlog=" + nginxLog);
        return null;
      }

      Double upstreamResponseTime = 0d;

      try {
        upstreamResponseTime = Double.parseDouble(dataList.get(S1Enum.UPSTREAM_RESPONSE_TIME.ordinal()));
      } catch (Exception e) {
        upstreamResponseTime = -1d;
      }

      Double d = upstreamResponseTime * 1000;

      NginxLogModel model = new NginxLogModel();

      model.setBizType(bizType);
      model.setIp(getIp(dataList.get(S1Enum.IPS.ordinal())));
      model.setMinute(minute);
      model.setUrl(url);
      model.setStatus(dataList.get(S1Enum.STATUS.ordinal()));
      model.setUpstreamResponseTime(d.intValue());

      return model;
    }

    log.warn("bizType error, nginxlog=" + nginxLog);
    return null;
  }

  public static List<String> getDataList(String nginxLog, String patternStr) {
    Pattern p = Pattern.compile(patternStr);
    Matcher m = p.matcher(nginxLog);

    List<String> dataList = new ArrayList();

    while (m.find()) {
      for (int i = 1; i <= m.groupCount(); i++) {
        dataList.add(m.group(i));
      }
    }

    return dataList;
  }

  public static String getUrl(String orgUrl) {
    Pattern p = Pattern.compile("(.*?) (.*?) (.*?)$");
    Matcher m = p.matcher(orgUrl);

    String url="";

    if (m.find()) {
      url = m.group(2);
    }

    if (StringUtils.isEmpty(url)) {
      return "";
    }

    String[] s = url.split("\\?");

    return s[0];
  }

  public static String getIp(String ips) {
    if (StringUtils.isEmpty(ips)) {
      return "";
    }

    String[] s=ips.split(", ");
    return s[0];
  }

  public static String getMinute(String nginxTime) {
    Date date;

    SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    try {
      date = formatter.parse(nginxTime);
    } catch (Exception e) {
      return null;
    }

    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
    return format.format(date);
  }

  public static void main(String[] args)
  {
    String str = "s1 219.138.139.139 - - [17/Feb/2017:12:11:30 +0800] \"POST /usercenter/appusedetail HTTP/1.1\" 200 51 \"\" \"HttpComponents/1.1\" 0.003 0.003 10.8.1.6";
    NginxLogModel model = ModelFactory.getModel(str);
    System.out.print(model);
  }
}
