import com.opensource.nebula.reader.NebulaReader;
import com.opensource.nebula.sync.ModuleClassLoader;
import com.opensource.nebula.writer.NebulaWriter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class Test {

    public static void main(String[] args) throws Exception {
        NebulaReader nebulaReader = getNebulaReader("1.x", "1234");
        NebulaWriter nebulaWriter = getNebulaWriter("3.x", "1234");
        System.out.println(nebulaReader.supportVersions());
        System.out.println(nebulaWriter.supportVersion());
    }

    private static NebulaReader getNebulaReader(String version, String graphAddress) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL readUrl = new File("nebula-reader/nebula-reader-{version}/nebula-reader-{version}/nebula-reader-{version}-1.0-SNAPSHOT-jar-with-dependencies.jar".replaceAll("\\{version\\}", version)).toURL();
        ModuleClassLoader moduleClassLoader = new ModuleClassLoader(Arrays.asList(readUrl), contextClassLoader, Arrays.asList(NebulaReader.class.getPackage().getName()));
        Thread.currentThread().setContextClassLoader(moduleClassLoader);
        Class<NebulaReader> readerClassList = findClassesImplementing(NebulaReader.class, moduleClassLoader);
        if (readerClassList == null) {
            throw new IllegalArgumentException("Not found NebulaReader implement class from " + readUrl);
        }
        Constructor<?> constructor = readerClassList.getConstructor(String.class);
        return (NebulaReader) constructor.newInstance(graphAddress);
    }

    private static NebulaWriter getNebulaWriter(String version, String metaAddress) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL readUrl = new File("nebula-writer/nebula-writer-{version}/nebula-writer-{version}/nebula-writer-{version}-1.0-SNAPSHOT-jar-with-dependencies.jar".replaceAll("\\{version\\}", version)).toURL();
        ModuleClassLoader moduleClassLoader = new ModuleClassLoader(Arrays.asList(readUrl), contextClassLoader, Arrays.asList(NebulaWriter.class.getPackage().getName()));
        Thread.currentThread().setContextClassLoader(moduleClassLoader);
        Class<NebulaWriter> readerClassList = findClassesImplementing(NebulaWriter.class, moduleClassLoader);
        if (readerClassList == null) {
            throw new IllegalArgumentException("Not found NebulaWriter implement class from " + readUrl);
        }
        Constructor<?> constructor = readerClassList.getConstructor(String.class);
        return (NebulaWriter) constructor.newInstance(metaAddress);
    }


    public static <T> Class<T> findClassesImplementing(Class<T> interfaceOrSuperclass, ClassLoader classLoader) throws IOException, ClassNotFoundException {
        String packageName = interfaceOrSuperclass.getPackage().getName();
        String packagePath = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(packagePath);
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            if (url != null) {
                String protocol = url.getProtocol();
                if (protocol.equals("file")) {
                    // 处理普通文件系统中的类
                    String filePath = url.getFile();
                    Class<T> classesInFileSystem = findClassesInFileSystem(packageName, filePath, interfaceOrSuperclass);
                    if (classesInFileSystem != null) {
                        return classesInFileSystem;
                    }
                } else if (protocol.equals("jar")) {
                    // 处理在 Jar 包中的类
                    JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
                    JarFile jarFile = jarURLConnection.getJarFile();
                    Class<T> classesInJar = findClassesInJar(jarFile, interfaceOrSuperclass);
                    if (classesInJar != null) {
                        return classesInJar;
                    }
                }
            }
        }
        return null;
    }

    private static <T> Class<T> findClassesInFileSystem(String packageName, String packagePath, Class<T> interfaceOrSuperclass) throws ClassNotFoundException {
        File packageDirectory = new File(packagePath);
        if (packageDirectory.exists() && packageDirectory.isDirectory()) {
            File[] files = packageDirectory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        return findClassesInFileSystem(packageName + "." + file.getName(), file.getAbsolutePath(), interfaceOrSuperclass);
                    } else if (file.getName().endsWith(".class")) {
                        String className = packageName + '.' + file.getName().substring(0, file.getName().length() - 6);
                        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
                        if (interfaceOrSuperclass.isAssignableFrom(clazz)
                                && !clazz.isInterface()
                                && (clazz.getModifiers() & Modifier.ABSTRACT) == 0
                                && !clazz.equals(interfaceOrSuperclass)) {
                            return (Class<T>) clazz;
                        }
                    }
                }
            }
        }
        return null;
    }

    private static <T> Class<T> findClassesInJar(JarFile jarFile, Class<T> interfaceOrSuperclass) {
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String entryName = entry.getName();
            if (entryName.endsWith(".class")) {
                String className = entryName.substring(0, entryName.length() - 6).replace('/', '.');
                try {
                    Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
                    if (interfaceOrSuperclass.isAssignableFrom(clazz)
                            && !clazz.isInterface()
                            && (clazz.getModifiers() & Modifier.ABSTRACT) == 0
                            && !clazz.equals(interfaceOrSuperclass)) {
                        return (Class<T>) clazz;
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

}
