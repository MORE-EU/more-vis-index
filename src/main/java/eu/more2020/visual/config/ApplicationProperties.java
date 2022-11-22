package eu.more2020.visual.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties specific to MoreVis.
 * <p>
 * Properties are configured in the {@code application.yml} file.
 */
@ConfigurationProperties(prefix = "application", ignoreUnknownFields = true)
public class ApplicationProperties {
    String workspacePath;
    String delimiter;
    String timeFormat;
    String toolApi;

    public String getWorkspacePath() {
        return workspacePath;
    }

    public void setWorkspacePath(String workspacePath) {
        this.workspacePath = workspacePath;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public String getToolApi() {
        return toolApi;
    }

    public void setToolApi(String toolApi) {
        this.toolApi = toolApi;
    }
}
