package test;

import java.util.ArrayList;
import java.util.Scanner;

public class HeapSort {

    private int size;

    private int getParent(int index) {
        return (index - 1) / 2;
    }

    private int getLeftChild(int index) {
        return 2 * index + 1;
    }

    private int getRightChild(int index) {
        return 2 * index + 2;
    }

    private void ajustHeap(Integer[] arr, int index) {
        while (true) {
            int lc = getLeftChild(index);
            int rc = getRightChild(index);
            if (lc >= size && rc >= size) {
                // index所指示的位置已是叶子结点，不用调整
                return;
            } else if (lc < size && rc >= size) {
                // 左斜树，即：有左子节点却没有右子节点
                if (arr[index] < arr[lc]) {
                    swap(arr, index, lc);
                    index = lc;
                } else {
                    return;
                }
            } else {
                // 拥有两个子节点的内部结点
                int maxValue, maxIndex;
                if (arr[lc] < arr[rc]) {
                    maxValue = arr[rc];
                    maxIndex = rc;
                } else {
                    maxValue = arr[lc];
                    maxIndex = lc;
                }
                if (arr[index] < maxValue) {
                    swap(arr, index, maxIndex);
                    index = maxIndex;
                } else {
                    // index已经调整至合适的位置
                    return;
                }
            }

        }

    }

    private void swap(Integer[] arr, int index1, int index2) {
        Integer tmp = arr[index1];
        arr[index1] = arr[index2];
        arr[index2] = tmp;
    }

    private void build(Integer[] arr) {
        for (int i = (size - 1) / 2;i >= 0;i--) {
            ajustHeap(arr, i);
        }
    }

    private Integer removeTop(Integer[] arr) {
        if (size == 0) {
            return null;
        }
        swap(arr, 0, size - 1);
        size--;
        ajustHeap(arr, 0);
        return arr[size];
    }

    public void sort(Integer[] arr) {
        size = arr.length;
        build(arr);
        Integer top;

        while ((top = removeTop(arr)) != null) {
            System.out.print(top + " ");
        }
        System.out.println();
    }


    public static void main(String[] args) {
        HeapSort heapSort = new HeapSort();
        Scanner s = new Scanner(System.in);
        ArrayList<Integer> list = new ArrayList<>();
        while (s.hasNext()) {
            list.add(s.nextInt());
        }
        Integer[] buf = new Integer[list.size()];
        heapSort.sort(list.toArray(buf));
    }
}

