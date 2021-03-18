package cn.thinkingdata.kafka.close;

import java.util.Scanner;

public class ScanTermMethod implements TermMethod {

    Scanner scan = new Scanner(System.in);

    @Override
    public Boolean receiveTermSignal() {
        System.out.println("Stop it now?(yes/no):");
        String result = scan.nextLine();
        return result.equals("yes");

    }

    @Override
    public void afterDestroyConsumer() {
        scan.close();
    }

}
