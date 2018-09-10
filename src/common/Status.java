package common;

public enum Status {
    ALLOCATE(0),    // 已分配
    LOAD(1),        // 已加载
    NEW(2),         // 已新建
    UPDATE(3),      // 已更新
    DELETE(4);      // 已删除


    /**
     * 枚举的内部值
     */
    private int value;

    private Status(int value) {
        this.value = value;
    }
}
