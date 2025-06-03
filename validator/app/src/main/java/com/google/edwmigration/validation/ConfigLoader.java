package com.google.edwmigration.validation;

import com.moandjiezana.toml.Toml;
import java.io.File;

public class ConfigLoader {

    public static ValidationConfig load(String path) {
        File configFile = new File(path);
        if (!configFile.exists()) {
            throw new IllegalArgumentException("Config file does not exist: " + path);
        }

        Toml toml = new Toml().read(configFile);
        return toml.to(ValidationConfig.class);
    }
}

