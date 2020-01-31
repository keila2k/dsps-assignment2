package dto;

public class Task {
    private String filename;
    private String data;

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Task(String filename, String data) {
        this.filename = filename;
        this.data = data;
    }
}
