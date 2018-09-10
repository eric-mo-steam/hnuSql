package structure;

public class Field {
    /**
     * 字段类型
     */
    private FieldType fieldType;
    /**
     * 字段名称
     */
    private String name;
    /**
     * 列字面值的最大长度
     */
    private int length;
    /**
     * 是否主键
     */
    private boolean isPrimary;
    /**
     * 是否是索引
     */
    private boolean isKey;

    public Field(String name, int length, FieldType fieldType) {
        this.name = name;
        this.length = length;
        this.fieldType = fieldType;
    }

    public Field() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

}
