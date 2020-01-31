package dto;

public class MessageDto {
    private MESSAGE_TYPE type;
    private String data;
//    TODO: change data to List<String>

    public MessageDto(MESSAGE_TYPE message_type, String data) {
        this.type = message_type;
        this.data = data;
    }

    public MESSAGE_TYPE getType() {
        return type;
    }

    public void setType(MESSAGE_TYPE type) {
        this.type = type;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
