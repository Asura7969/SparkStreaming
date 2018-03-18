package saveOffset.model;

public class UrlMinuteIpsModel extends BaseModel
{
  private String minute;
  private String url;
  private String ips;

  public String getMinute() {
    return minute;
  }

  public void setMinute(String minute) {
    this.minute = minute;
  }

  public String getUrl()
  {
    return url;
  }

  public void setUrl(String url)
  {
    this.url = url;
  }

  public String getIps()
  {
    return ips;
  }

  public void setIps(String ips)
  {
    this.ips = ips;
  }
}
