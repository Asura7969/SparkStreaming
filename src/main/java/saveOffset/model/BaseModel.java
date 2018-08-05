package saveOffset.model;

import java.io.Serializable;

public class BaseModel implements Serializable {
  public String bizType;

  public String getBizType()
  {
    return bizType;
  }

  public void setBizType(String bizType)
  {
    this.bizType = bizType;
  }
}
