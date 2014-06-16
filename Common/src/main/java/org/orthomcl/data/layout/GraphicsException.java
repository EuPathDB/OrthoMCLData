/**
 * 
 */
package org.orthomcl.data.layout;

/**
 * @author jerric
 * 
 */
public class GraphicsException extends Exception {

  /**
	 * 
	 */
  private static final long serialVersionUID = 8314218235612834227L;

  /**
	 * 
	 */
  public GraphicsException() {}

  /**
   * @param message
   */
  public GraphicsException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public GraphicsException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public GraphicsException(String message, Throwable cause) {
    super(message, cause);
  }

}
