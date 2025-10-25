package com.pyatkin.cassandra.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigReader {
    public static Map<String, String> read(String path) throws IOException {
        Map<String, String> map = new HashMap<>();
        List<String> lines = Files.readAllLines(Paths.get(path));
        for (String line : lines) {
            String l = line.trim();
            if (l.isEmpty() || l.startsWith("#")) continue;
            String[] parts = l.split(":", 2);
            if (parts.length == 2) {
                map.put(parts[0].trim(), parts[1].trim());
            }
        }
        return map;
    }
}
