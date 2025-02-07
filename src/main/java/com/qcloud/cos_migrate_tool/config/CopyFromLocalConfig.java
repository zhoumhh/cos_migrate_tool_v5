package com.qcloud.cos_migrate_tool.config;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import com.qcloud.cos_migrate_tool.utils.SystemUtils;

public class CopyFromLocalConfig extends CommonConfig {
    private String localPath;
    private Set<String> excludes = new HashSet<String>();
    private long ignoreModifiedTimeLessThan = -1;
    private Set<String> ignoreSuffixs = new HashSet<String>();
    private Set<String> includeSuffixs = new HashSet<String>();
    boolean ignoreEmptyFile = false;
    boolean fileListMode = false;
    private String fileListPath;

    public void setIgnoreEmptyFile(boolean ignoreEmptyFile) {
        this.ignoreEmptyFile = ignoreEmptyFile;
    }
    
    public void setIgnoreSuffix(String ignore) {
        ignore = ignore.trim();
        String[] ignoreArray = ignore.split(";");
        for (String ignoreElement : ignoreArray) {
            this.ignoreSuffixs.add(ignoreElement);
        }
    }

    public void setIncludeSuffix(String include) {
        include = include.trim();
        String[] includeArray = include.split(";");
        for (String includeElement : includeArray) {
            this.includeSuffixs.add(includeElement);
        }
    }
    
    public String needToMigrate(Path file, String localPath) {
        if (isExcludes(localPath)) {
            return "excludes";
        }
        
        for (String suffix:ignoreSuffixs) {
            if(localPath.endsWith(suffix)) {
                return "suffix";
            }
        }
        
        if (ignoreEmptyFile) {
            File localFile = new File(file.toString());
            if (localFile.length() == 0) {
                return "empty file";
            }
        }

        if (includeSuffixs.isEmpty()) {
            return "";
        }

        for (String suffix:includeSuffixs) {
            if (localPath.endsWith(suffix)) {
                return "";
            }
        }
        
        return "do not match include suffix";
    }
    
    public String getLocalPath() {
        return localPath;
    }

    public void setLocalPath(String localPath) throws IllegalArgumentException {
        File localPathFile = new File(localPath);
        if (!localPathFile.exists()) {
            throw new IllegalArgumentException("local path not exist!");
        }
        this.localPath = SystemUtils.formatLocalPath(localPath);
    }

    public void setExcludes(String excludePath) throws IllegalArgumentException {
        excludePath = excludePath.trim();
        String[] exludePathArray = excludePath.split(";");
        for (String excludePathElement : exludePathArray) {
            File tempFile = new File(excludePathElement);
            if (!tempFile.exists()) {
                throw new IllegalArgumentException("excludePath " + excludePath + " not exist");
            }
            this.excludes.add(SystemUtils.formatLocalPath(tempFile.getAbsolutePath()));
        }
    }

    public boolean isExcludes(String excludePath) {
        return this.excludes.contains(excludePath);
    }
    
    public void setIgnoreModifiedTimeLessThan(String ignoreModifiedTimeLessThanStr) {
        try {
            long number = Long.valueOf(ignoreModifiedTimeLessThanStr);
            if (number <= 0) {
                throw new IllegalArgumentException("legal ignoreModifiedTimeLessThan must be positive");
            }
            this.ignoreModifiedTimeLessThan = number;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid ignoreModifiedTimeLessThan");
        }
    }

    public long getIgnoreModifiedTimeLessThan() {
        return ignoreModifiedTimeLessThan;
    }
    public boolean isFileListMode() {
        return fileListMode;
    }

    public void setFileListMode(boolean fileListMode) {
        this.fileListMode = fileListMode;
    }

    public String getFileListPath() {
        return fileListPath;
    }

    public void setFileListPath(String fileListPath) {
        this.fileListPath = fileListPath;
    }
}
