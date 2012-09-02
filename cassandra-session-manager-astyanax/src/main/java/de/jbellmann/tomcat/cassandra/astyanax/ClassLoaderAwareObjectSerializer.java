package de.jbellmann.tomcat.cassandra.astyanax;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;

import com.netflix.astyanax.connectionpool.exceptions.SerializationException;
import com.netflix.astyanax.serializers.ObjectSerializer;

/**
 * We need the {@link ClassLoader} to create the Objects we have in the
 * Webapp otherwise we got a {@link ClassNotFoundException}.
 * 
 * Taken from Hector.
 * 
 * @author Joerg Bellmann
 *
 */
public class ClassLoaderAwareObjectSerializer extends ObjectSerializer {

    private final ClassLoader classLoader;

    public ClassLoaderAwareObjectSerializer(ClassLoader classLoader) {
        super();
        this.classLoader = classLoader;
    }

    @Override
    public Object fromByteBuffer(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        }
        try {
            int l = bytes.remaining();
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes.array(), bytes.arrayOffset() + bytes.position(), l);
            ObjectInputStream ois;
            if (classLoader != null) {
                ois = new CustomClassLoaderObjectInputStream(classLoader, bais);
            } else {
                ois = new ObjectInputStream(bais);
            }
            Object obj = ois.readObject();
            bytes.position(bytes.position() + (l - ois.available()));
            ois.close();
            return obj;
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }

    /**
     * Object input stream that uses a custom class loader to resolve classes 
     */
    static class CustomClassLoaderObjectInputStream extends ObjectInputStream {

        private final ClassLoader classLoader;

        CustomClassLoaderObjectInputStream(ClassLoader classLoader, InputStream is) throws IOException {
            super(is);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
            return Class.forName(desc.getName(), false, classLoader);
        }

    }
}
