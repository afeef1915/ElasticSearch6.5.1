package org.elasticsearch.bootstrap;
import java.net.URL;
import java.util.Set;
public class JarHell {
    private JarHell() {}
    public static void checkJarHell() throws Exception {}
    public static void checkJarHell(URL urls[]) throws Exception {}
    public static void checkVersionFormat(String targetVersion) {}
    public static void checkJavaVersion(String resource, String targetVersion) {}
    public static Set<URL> parseClassPath() {return ImmutableSet.of();}
}