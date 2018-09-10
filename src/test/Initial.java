package test;

public class Initial {
    public static void main(String[] args) {
        SuperClass[] arr = new SuperClass[10];
        System.out.println(arr.toString());
    }
}

class SuperClass{
    public static int value = 123;
    static {
        System.out.println("SuperClass initial");
    }
}

class SubClass extends SuperClass {
    public static int value = 234;
    static {
        System.out.println("SubClass initial");
    }
}
