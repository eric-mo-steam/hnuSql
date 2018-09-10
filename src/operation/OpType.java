package operation;

public enum OpType {
    CREATE(1, "create"),
    INSERT(2, "insert"),
    UPDATE(3, "update"),
    SELECT(4, "select");

    private int value;
    private String name;

    private OpType(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static OpType valueOf(int value) {
        for (OpType opType : OpType.values()) {
            if (opType.value == value) {
                return opType;
            }
        }
        return null;
    }
}
