package org.apache.cassandra.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public final class JavaInternals {
    public static final Unsafe unsafe = getUnsafe();
    public static final long byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);

    public static Unsafe getUnsafe() {
        try {
            return (Unsafe) getField(Unsafe.class, "theUnsafe").get(null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Field getField(Class<?> cls, String name) {
        try {
            Field f = cls.getDeclaredField(name);
            f.setAccessible(true);
            return f;
        } catch (Exception e) {
            return null;
        }
    }

    public static Field getField(String cls, String name) {
        try {
            return getField(Class.forName(cls), name);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    public static Field findFieldRecursively(Class<?> cls, String name) {
        for (; cls != null; cls = cls.getSuperclass()) {
            Field f = getField(cls, name);
            if (f != null) {
                return f;
            }
        }
        return null;
    }

    public static Method getMethod(Class<?> cls, String name, Class... params) {
        try {
            Method m = cls.getDeclaredMethod(name, params);
            m.setAccessible(true);
            return m;
        } catch (Exception e) {
            return null;
        }
    }

    public static Method getMethod(String cls, String name, Class... params) {
        try {
            return getMethod(Class.forName(cls), name, params);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    public static Method findMethodRecursively(Class<?> cls, String name, Class... params) {
        for (; cls != null; cls = cls.getSuperclass()) {
            Method m = getMethod(cls, name, params);
            if (m != null) {
                return m;
            }
        }
        return null;
    }

    public static <T> Constructor<T> getConstructor(Class<T> cls, Class... params) {
        try {
            Constructor<T> c = cls.getDeclaredConstructor(params);
            c.setAccessible(true);
            return c;
        } catch (Exception e) {
            return null;
        }
    }

    public static Constructor<?> getConstructor(String cls, Class... params) {
        try {
            return getConstructor(Class.forName(cls), params);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    public static long fieldOffset(Class<?> cls, String name) {
        try {
            return unsafe.objectFieldOffset(cls.getDeclaredField(name));
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    public static long fieldOffset(String cls, String name) {
        try {
            return fieldOffset(Class.forName(cls), name);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    // Useful for patching final fields
    public static void setStaticField(Class<?> cls, String name, Object value) {
        try {
            Field field = cls.getDeclaredField(name);
            if (!Modifier.isStatic(field.getModifiers())) {
                throw new IllegalArgumentException("Static field expected");
            }
            unsafe.putObject(unsafe.staticFieldBase(field), unsafe.staticFieldOffset(field), value);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    // Useful for patching final fields
    public static void setObjectField(Object obj, String name, Object value) {
        try {
            Field field = obj.getClass().getDeclaredField(name);
            if (Modifier.isStatic(field.getModifiers())) {
                throw new IllegalArgumentException("Object field expected");
            }
            unsafe.putObject(obj, unsafe.objectFieldOffset(field), value);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <E extends Throwable> void uncheckedThrow(Throwable e) throws E {
        throw (E) e;
    }
}