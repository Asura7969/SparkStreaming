package saveOffset.key;

import scala.math.Ordered;

import java.io.Serializable;

public class BizMinuteUrlKey implements Ordered<BizMinuteUrlKey>, Serializable {
  private String bizType;
  private String minute;
  private String url;

  @Override
  public int compareTo(BizMinuteUrlKey that) {
    if (bizType.compareTo(that.getBizType()) == 0) {
      if (minute.compareTo(that.getMinute()) == 0) {
          return url.compareTo(that.getUrl());
      } else {
        return minute.compareTo(that.getMinute());
      }
    } else {
      return bizType.compareTo(that.getBizType());
    }
  }

  @Override
  public boolean $less(BizMinuteUrlKey that) {
    if (this.compareTo(that) < 0) {
      return true;
    }

    return false;
  }

  @Override
  public boolean $greater(BizMinuteUrlKey that) {
    if (this.compareTo(that) > 0) {
      return true;
    }

    return false;
  }

  @Override
  public boolean $less$eq(BizMinuteUrlKey that) {
    if (this.compareTo(that) <= 0) {
      return true;
    }

    return false;
  }

  @Override
  public boolean $greater$eq(BizMinuteUrlKey that) {
    if (this.compareTo(that) >= 0) {
      return true;
    }

    return false;
  }

  @Override
  public int compare(BizMinuteUrlKey that) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BizMinuteUrlKey that = (BizMinuteUrlKey) o;

    if (bizType != null ? !bizType.equals(that.bizType) : that.bizType != null) return false;
    if (minute != null ? !minute.equals(that.minute) : that.minute != null) return false;
    return !(url != null ? !url.equals(that.url) : that.url != null);

  }

  @Override
  public int hashCode() {
    int result = bizType != null ? bizType.hashCode() : 0;
    result = 31 * result + (minute != null ? minute.hashCode() : 0);
    result = 31 * result + (url != null ? url.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "BizMinuteUrlKey{" +
            "bizType='" + bizType + '\'' +
            ", minute='" + minute + '\'' +
            ", url='" + url + '\'' +
            '}';
  }
}
