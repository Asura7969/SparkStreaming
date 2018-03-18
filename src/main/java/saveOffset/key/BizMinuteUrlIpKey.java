package saveOffset.key;

import scala.math.Ordered;

import java.io.Serializable;

public class BizMinuteUrlIpKey implements Ordered<BizMinuteUrlIpKey>, Serializable {
  private String bizType;
  private String minute;
  private String url;
  private String ip;

  @Override
  public int compareTo(BizMinuteUrlIpKey that) {
    if (this.bizType.compareTo(that.getBizType()) == 0) {
      if (this.minute.compareTo(that.getMinute()) == 0) {
          if (this.url.compareTo(that.getUrl()) == 0) {
            return this.ip.compareTo(that.getIp());
          } else {
            return this.url.compareTo(that.getUrl());
          }
      } else {
        return this.minute.compareTo(that.getMinute());
      }
    } else {
      return this.bizType.compareTo(that.getBizType());
    }
  }

  @Override
  public boolean $less(BizMinuteUrlIpKey that) {
    if (this.compareTo(that) < 0) {
      return true;
    }

    return false;
  }

  @Override
  public boolean $greater(BizMinuteUrlIpKey that) {
    if (this.compareTo(that) > 0) {
      return true;
    }

    return false;
  }

  @Override
  public boolean $less$eq(BizMinuteUrlIpKey that) {
    if (this.compareTo(that) <= 0) {
      return true;
    }

    return false;
  }

  @Override
  public boolean $greater$eq(BizMinuteUrlIpKey that) {
    if (this.compareTo(that) >= 0) {
      return true;
    }

    return false;
  }

  @Override
  public int compare(BizMinuteUrlIpKey that) {
    return this.compareTo(that);
  }

  public String getBizType() {
    return bizType;
  }

  public void setBizType(String bizType) {
    this.bizType = bizType;
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

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BizMinuteUrlIpKey that = (BizMinuteUrlIpKey) o;

    if (bizType != null ? !bizType.equals(that.bizType) : that.bizType != null) return false;
    if (minute != null ? !minute.equals(that.minute) : that.minute != null) return false;
    if (url != null ? !url.equals(that.url) : that.url != null) return false;
    return !(ip != null ? !ip.equals(that.ip) : that.ip != null);

  }

  @Override
  public int hashCode() {
    int result = bizType != null ? bizType.hashCode() : 0;
    result = 31 * result + (minute != null ? minute.hashCode() : 0);
    result = 31 * result + (url != null ? url.hashCode() : 0);
    result = 31 * result + (ip != null ? ip.hashCode() : 0);
    return result;
  }
}
