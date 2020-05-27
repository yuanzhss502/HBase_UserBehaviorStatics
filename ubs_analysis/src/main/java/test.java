public class test {

    public static String reverse1(String str) {

        StringBuilder stringBuilder = new StringBuilder(str);
        return stringBuilder.reverse().toString();

    }

    public static String reverse2(String str2) {

        char[] charArray = str2.toCharArray();
        String reverse = "";
        for (int i=charArray.length -1; i >= 0; i--) {

            reverse += charArray[i];

        }
        return reverse;

    }

    public static String reverse3(String str) {

        int length = str.length();
        String reverse = "";
        for (int i = 0; i<= length -1; i++) {
            System.out.println(str.charAt(i));
            reverse = str.charAt(i) + reverse;
        }
        return reverse;
    }

    public static String reverse4(String s) {

        int length = s.length();
        if(length <= 1){
            return s;
        }

        String left = s.substring(0, length / 2);

        String right = s.substring(length / 2, length);

        return reverse1(right) + reverse1(left);

    }

    public static void main(String[] args) {


        String str1 = new String("isdadhwe");
        System.out.println(reverse1(str1));

        System.out.println(reverse2(str1));

        System.out.println(reverse3(str1));
    }



}
