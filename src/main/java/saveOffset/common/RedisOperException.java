package saveOffset.common;

public class RedisOperException extends RuntimeException {

  /** CommentException */
  private static final long serialVersionUID = -69538135323395908L;

  private String errorCode;

  public RedisOperException()
  {
    super();
  }

  public RedisOperException(String errorCode, Throwable t)
  {
    this(errorCode, t, null);
  }

  public RedisOperException(String errorCode, Throwable t, String message)
  {
    super(message, t);
    this.errorCode = errorCode;
  }

  public RedisOperException(String errorCode)
  {
    this(errorCode, "");
  }

  public RedisOperException(String errorCode, String msg)
  {
    super(msg);
    this.errorCode = errorCode;
  }

  /**
   * Getter method for property <tt>errorCode</tt>.
   *
   * @return property value of errorCode
   */
  public String getErrorCode()
  {
    return errorCode;
  }

  /**
   * Setter method for property <tt>errorCode</tt>.
   *
   * @param errorCode
   *          value to be assigned to property errorCode
   */
  public void setErrorCode(String errorCode)
  {
    this.errorCode = errorCode;
  }

}
