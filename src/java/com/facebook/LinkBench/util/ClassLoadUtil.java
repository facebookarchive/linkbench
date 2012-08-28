package com.facebook.LinkBench.util;

import java.lang.reflect.Constructor;

/**
 * Utility methods for dynamic loading of classes
 */
public class ClassLoadUtil {

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
  
  /**
   * Load a class by name.
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public static Class<?> getClassByName(String name) 
                            throws ClassNotFoundException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return Class.forName(name, true, classLoader);
  }

  /** Create an object for the given class and initialize it from conf
   *
   * @param theClass class of which an object is created
   * @param expected the expected parent class or interface
   * @return a new object
   */
  public static <T> T newInstance(Class<?> theClass, Class<T> expected) {
    T result;
    try {
      if (!expected.isAssignableFrom(theClass)) {
        throw new Exception("Specified class " + theClass.getName() + "" +
        		"does not extend/implement " + expected.getName());
      }
      Class<? extends T> clazz = (Class<? extends T>)theClass;
      Constructor<? extends T> meth = clazz.getDeclaredConstructor(EMPTY_ARRAY);
      meth.setAccessible(true);
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }
  
  public static <T> T newInstance(String className, Class<T> expected)
                                        throws ClassNotFoundException {
    return newInstance(getClassByName(className), expected);
  }
}
