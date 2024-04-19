package com.opensource.nebula.sync;

import com.opensource.nebula.reader.EdgeDomain;
import com.opensource.nebula.reader.NebulaReader;
import com.opensource.nebula.reader.TagDomain;
import com.opensource.nebula.writer.NebulaWriter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

public class NebulaDataSync {

    private final String readerJarDirPath;
    private final String readerNebulaVersion;
    private final String readerNebulaMetaAddress;

    private final String writeJarDirPath;
    private final String writerNebulaVersion;
    private final String writerNebulaGraphAddress;

    private final String space;
    private int scanLimit = 100;
    private long startTime = 0;
    private long endTime = Long.MAX_VALUE;
    private Executor threadPool = null;

    public NebulaDataSync(String readerJarDirPath, String readerNebulaVersion, String readerNebulaMetaAddress, String writeJarDirPath, String writerNebulaVersion, String writerNebulaGraphAddress, String space) {
        assert readerJarDirPath != null && readerJarDirPath.length() == 0 : "readerJarDirPath is empty";
        this.readerJarDirPath = readerJarDirPath;
        assert readerNebulaVersion != null && readerNebulaVersion.length() == 0 : "readerNebulaVersion is empty";
        this.readerNebulaVersion = readerNebulaVersion;
        assert readerNebulaMetaAddress != null && readerNebulaMetaAddress.length() == 0 : "readerNebulaMetaAddress is empty";
        this.readerNebulaMetaAddress = readerNebulaMetaAddress;
        assert writeJarDirPath != null && writeJarDirPath.length() == 0 : "writeJarDirPath is empty";
        this.writeJarDirPath = writeJarDirPath;
        assert writerNebulaVersion != null && writerNebulaVersion.length() == 0 : "writerNebulaVersion is empty";
        this.writerNebulaVersion = writerNebulaVersion;
        assert writerNebulaGraphAddress != null && writerNebulaGraphAddress.length() == 0 : "writerNebulaGraphAddress is empty";
        this.writerNebulaGraphAddress = writerNebulaGraphAddress;
        assert space != null && space.length() == 0 : "space is empty";
        this.space = space;
    }

    public void startSync() throws Exception {
        threadPool = Optional.ofNullable(threadPool).orElseGet(() -> Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("nebula-data-sync")));
        try (NebulaReader nebulaReader = getNebulaReader(this.readerNebulaVersion, this.writerNebulaGraphAddress);
             NebulaWriter nebulaWriter = getNebulaWriter(this.writerNebulaVersion, this.readerNebulaMetaAddress)) {
            syncEdge(nebulaReader, nebulaWriter);
            syncVertex(nebulaReader, nebulaWriter);
        }
    }

    private void syncEdge(NebulaReader nebulaReader, NebulaWriter nebulaWriter) {
        List<EdgeDomain> allEdgeList = nebulaReader.getAllEdgeList(space);
        List<Integer> allPartList = nebulaReader.getAllPartList(space);
        Map<String, List<String>> edgeColumn = allEdgeList.stream().collect(Collectors.toMap(EdgeDomain::getEdgeName, edgeDomain -> new ArrayList<>()));
        for (Integer part : allPartList) {
            threadPool.execute(() -> {
                Iterator<List<String>> edgeIterator = nebulaReader.scanEdge(space, part, edgeColumn, scanLimit, startTime, endTime);
                while (edgeIterator.hasNext()) {
                    List<String> edgeInsertStatementList = edgeIterator.next();
                    nebulaWriter.executeStatement(edgeInsertStatementList);
                }
            });
        }
    }

    private void syncVertex(NebulaReader nebulaReader, NebulaWriter nebulaWriter) {
        List<TagDomain> allEdgeList = nebulaReader.getAllTagList(space);
        List<Integer> allPartList = nebulaReader.getAllPartList(space);
        Map<String, List<String>> tagColumn = allEdgeList.stream().collect(Collectors.toMap(TagDomain::getTagName, tagDomain -> new ArrayList<>()));
        for (Integer part : allPartList) {
            threadPool.execute(() -> {
                Iterator<List<String>> tagIterator = nebulaReader.scanVertex(space, part, tagColumn, scanLimit, startTime, endTime);
                while (tagIterator.hasNext()) {
                    List<String> tagInsertStatementList = tagIterator.next();
                    nebulaWriter.executeStatement(tagInsertStatementList);
                }
            });
        }
    }


    private NebulaReader getNebulaReader(String version, String graphAddress) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL readUrl = new File(readerJarDirPath + "/nebula-reader-{version}-1.0-SNAPSHOT-jar-with-dependencies.jar".replaceAll("\\{version\\}", version)).toURL();
        ModuleClassLoader moduleClassLoader = new ModuleClassLoader(Arrays.asList(readUrl), contextClassLoader, Arrays.asList(NebulaReader.class.getPackage().getName()));
        Thread.currentThread().setContextClassLoader(moduleClassLoader);
        Class<NebulaReader> readerClassList = findClassesImplementing(NebulaReader.class, moduleClassLoader);
        if (readerClassList == null) {
            throw new IllegalArgumentException("Not found NebulaReader implement class from " + readUrl);
        }
        Constructor<?> constructor = readerClassList.getConstructor(String.class);
        return (NebulaReader) constructor.newInstance(graphAddress);
    }

    private NebulaWriter getNebulaWriter(String version, String metaAddress) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL readUrl = new File(writeJarDirPath + "/nebula-writer-{version}-1.0-SNAPSHOT-jar-with-dependencies.jar".replaceAll("\\{version\\}", version)).toURL();
        ModuleClassLoader moduleClassLoader = new ModuleClassLoader(Arrays.asList(readUrl), contextClassLoader, Arrays.asList(NebulaWriter.class.getPackage().getName()));
        Thread.currentThread().setContextClassLoader(moduleClassLoader);
        Class<NebulaWriter> readerClassList = findClassesImplementing(NebulaWriter.class, moduleClassLoader);
        if (readerClassList == null) {
            throw new IllegalArgumentException("Not found NebulaWriter implement class from " + readUrl);
        }
        Constructor<?> constructor = readerClassList.getConstructor(String.class);
        return (NebulaWriter) constructor.newInstance(metaAddress);
    }


    public <T> Class<T> findClassesImplementing(Class<T> interfaceOrSuperclass, ClassLoader classLoader) throws IOException, ClassNotFoundException {
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

    private <T> Class<T> findClassesInFileSystem(String packageName, String packagePath, Class<T> interfaceOrSuperclass) throws ClassNotFoundException {
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

    private <T> Class<T> findClassesInJar(JarFile jarFile, Class<T> interfaceOrSuperclass) {
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

    public void setScanLimit(int scanLimit) {
        this.scanLimit = scanLimit;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setThreadPool(Executor threadPool) {
        this.threadPool = threadPool;
    }
}
