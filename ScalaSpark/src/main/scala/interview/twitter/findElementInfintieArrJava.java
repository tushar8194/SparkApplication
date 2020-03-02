package interview.twitter;

import java.util.*;

public class findElementInfintieArrJava {

    public static void main(String[] args) {

        int inf = Integer.MAX_VALUE;

        int[] A  = new int[]{1,4,6,8, inf,inf,inf,inf,inf,inf,inf};

        int res = findElement(A,6);

        System.out.println(res);

    }


    public static int findElement(int[] array_of_elemets, int x) {
        int rtrn =   Arrays.binarySearch(array_of_elemets,x);

        if(rtrn <0 ){
            return -1;
        }else{
            return rtrn;
        }
    }
}
