package saveOffset.model;

public class NginxLogModel extends BaseModel
{
  private String ip;
  private String minute;
  private String url;
  private String status;
  private Integer upstreamResponseTime;

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public String getMinute() {
    return minute;
  }

  public void setMinute(String minute) {
    this.minute = minute;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Integer getUpstreamResponseTime() {
    return upstreamResponseTime;
  }

  public void setUpstreamResponseTime(Integer upstreamResponseTime) {
    this.upstreamResponseTime = upstreamResponseTime;
  }

  @Override
  public String toString() {
    return "NginxLogModel{" +
            "ip='" + ip + '\'' +
            ", minute='" + minute + '\'' +
            ", url='" + url + '\'' +
            ", status='" + status + '\'' +
            ", upstreamResponseTime=" + upstreamResponseTime +
            '}';
  }
}
