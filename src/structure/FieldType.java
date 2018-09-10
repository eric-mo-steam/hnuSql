package structure;

public enum FieldType {
    INTEGER(1),
    CHAR(2);

    /**
     * 枚举的值
     */
    private int value;

    private FieldType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public static FieldType valueOf(int value) {
        for (FieldType fieldType : FieldType.values()) {
            if (fieldType.value == value) {
                return fieldType;
            }
        }
        return null;
    }
}
