package com.ververica.utils;

import com.ververica.models.ClickEvent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.stream.Stream;

public class DataSourceUtils {
    public static Stream<String> loadDataFile(String fileName) throws IOException {
        return Files.lines(
                Paths.get(System.getProperty("user.dir") + fileName)
        ).skip(1);
    }

    public static ClickEvent toClickEvent(String line) {
        String[] tokens = line.split(",", -1);
        Timestamp timestamp = Timestamp.valueOf(tokens[0].replace(" UTC", ""));
        if (tokens[8].equals("")) {
            tokens[8] = "dd5a3d71-0fb8-4c62-a046-cf234dd6e2b5";
        }
        return new ClickEvent(
                timestamp.getTime(),
                tokens[1],
                tokens[2],
                tokens[3],
                tokens[4],
                tokens[5],
                Double.parseDouble(tokens[6]),
                tokens[7],
                tokens[8]
        );
    }
}