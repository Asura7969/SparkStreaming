package saveOffset.model;

public class UrlMinutePvModel extends BaseModel
{
  private String minute;
  private String url;
  private Long pv;

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

  public Long getPv()
  {
    return pv;
  }

  public void setPv(Long pv)
  {
    this.pv = pv;
  }
}
