package org.example;

public class Test {
    public int removeDuplicates(int[] nums) {
        int index2 = 0;
        int index = nums[0];
        for(int i = 0; i < nums.length; i++){
            if(nums[i] > index){
                nums[index2+1] = nums[i];
                index = nums[i];
                index2++;
                i = index2;
            }
        }
        return index2;
    }
    public static void main(String[] args) {
        int[] arr = {1,1,2};
        Test test = new Test();
        test.removeDuplicates(arr);
    }
}


