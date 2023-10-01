import jdk.swing.interop.SwingInterOpUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class Controller {
    //list of dstores
    public static List<String> dstoreList = new ArrayList<String>();
    //dstore + port
    public static Map<String, Integer> dstoreStatus = new HashMap<>();
    public static Map<Integer, String> d2 = new HashMap<>();
    //file name + list of dstores
    public static Map<String, List<Integer>> fileLocations = new HashMap<>();
    //shared instance of index class
    public static Index index = new Index();
    //File name + File size
    public static Map<String, String> fileSize = new HashMap<String, String>();
    //port + files
    public static Map<Integer, Integer> noOfFiles = new HashMap<>();

    public static Boolean waiting = false;
    public static AtomicInteger storeAcks = new AtomicInteger(0);
    public static AtomicInteger removeAcks = new AtomicInteger(0);

    public static Map<String, ControllerThread> dstoreThreadMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        int cport = Integer.parseInt(args[0]); // Port number for controller
        int R = Integer.parseInt(args[1]); // Some value for R
        int timeout = Integer.parseInt(args[2]); // Timeout value
        int rebalance_period = Integer.parseInt(args[3]); // Rebalance period

        // Create a server socket
        ServerSocket serverSocket = new ServerSocket(cport);
        System.out.println("Controller listening on port " + cport);

        while (true) {
            // Wait for a connection from a Dstore
            Socket dstoreSocket = serverSocket.accept();
            String dstoreEndpoint = dstoreSocket.getRemoteSocketAddress().toString();
            System.out.println("Controller received connection from Dstore at " + dstoreEndpoint);

            // Create a new thread to handle communication with the Dstore
            Thread thread = new Thread(new ControllerThread(dstoreSocket, R, timeout, rebalance_period));
            thread.start();
        }
    }
}

class ControllerThread implements Runnable {
    private static final Object fileLock = new Object();
    private Socket dstoreSocket;
    private int R;
    private int timeout;
    private int rebalance_period;
    private int reloadCounter=0;
    private PrintWriter out;
    private int portno;
    private boolean isDstore =false;

    public ControllerThread(Socket dstoreSocket, int R, int timeout, int rebalance_period) throws IOException {
        this.dstoreSocket = dstoreSocket;
        this.R = R;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        this.out  = new PrintWriter(dstoreSocket.getOutputStream(), true);
    }

    @Override
    public void run() {
        System.out.println("Controller thread " + Thread.currentThread().getId() + " started for Dstore at " + dstoreSocket.getRemoteSocketAddress());
        try {
            // Read message sent by the dstore
            BufferedReader in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
//            PrintWriter out = new PrintWriter(dstoreSocket.getOutputStream(), true);
            String message;

            while ((message = in.readLine()) != null) {
                System.out.println(message + " received");
                String[] parts = message.split(" ");
                String command = parts[0];

                // Check if the message is a "JOIN" message
                if (command.equals("JOIN")) {
                    isDstore=true;
                    int port;
                    try{
                        port = Integer.parseInt(parts[1]);}
                    catch (NumberFormatException e) {
                        port = 0;
                    }
                    this.portno=port;

                    String dstoreEndpoint = dstoreSocket.getRemoteSocketAddress().toString();
                    System.out.println("Controller received JOIN message from Dstore at " + dstoreEndpoint);

                    // Add the Dstore to the list of Dstores
                    synchronized (Controller.dstoreList) {
                        Controller.dstoreList.add(dstoreEndpoint);
                        Controller.dstoreStatus.put(dstoreEndpoint, port);
                        Controller.d2.put(port, dstoreEndpoint);
                        Controller.noOfFiles.put(port,0);
                        Controller.dstoreThreadMap.put(dstoreEndpoint,ControllerThread.this);
                    }
                }

                if (command.equals("LIST")) {
                    if (Controller.dstoreList.size() < R) {
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                        System.out.println("ERROR_NOT_ENOUGH_DSTORES");
                    }else {
                        System.out.println("Controller received LIST message");
                        List<String> files = Controller.index.getStoredFiles();
                        List<String> filesToSend = new ArrayList<>();
                        for(String f: files){
                            if(Controller.index.getState(f).equals(IndexState.STORED)){
                                filesToSend.add(f);
                            }
                        }
                        StringBuilder sb = new StringBuilder();
                        for (String s : files) {
                            sb.append(s);
                            sb.append(" ");
                        }
                        String result = sb.toString().trim();
                        System.out.println("LIST " + result);
                        out.println("LIST " + result);
                    }
                }
                if (command.equals("STORE")) {
                    synchronized (fileLock){
                    System.out.println("Controller received STORE message");
                    if (Controller.dstoreList.size() < R) {
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                        System.out.println("ERROR_NOT_ENOUGH_DSTORES");
                    } else if(Controller.index.getStoredFiles().contains(parts[1])){
                        out.println("ERROR_FILE_ALREADY_EXISTS");
                        System.out.println("ERROR_FILE_ALREADY_EXISTS");
                    }
                    else{
                        List<Integer> selectedDstores = new ArrayList<Integer>();
                        Controller.index.add(parts[1]);


                        List<Integer> result = new ArrayList<>();
                        int minCount = Integer.MAX_VALUE;
                        for (int id : Controller.noOfFiles.keySet()) {
                            int count = Controller.noOfFiles.get(id);
                            if (count < minCount) {
                                result.clear();
                                result.add(id);
                                minCount = count;
                            } else if (count == minCount) {
                                result.add(id);
                            }

//                      return result.subList(0, Math.min(numFiles, result.size()));
                        }
                        selectedDstores = result.subList(0, Math.min(R, result.size()));
                        System.out.println("Dstores:" + selectedDstores);


                        StringBuilder sb2 = new StringBuilder();
                        for (int s : selectedDstores) {
                            int files = Controller.noOfFiles.get(s);
                            Controller.noOfFiles.remove(s);
                            Controller.noOfFiles.put(s, (files + 1));
                            sb2.append(s);
                            sb2.append(" ");

                        }

                        System.out.println(sb2);
                        Controller.fileLocations.put(parts[1], selectedDstores);
                        Controller.fileSize.put(parts[1], parts[2]);
                        System.out.println(Controller.fileLocations);

                        // Send the selected Dstores to the client
                        String storeMessage = "STORE_TO " + sb2;
                        out.println(storeMessage);
                        System.out.println("Sent message to client: " + storeMessage);

                        Controller.waiting=true;
                        while (Controller.waiting) {
                            final boolean[] timeoutOccurred = {false};
                            Timer timer = new Timer();
                            timer.schedule(new TimerTask() {
                                @Override
                                public void run() {
                                    timeoutOccurred[0] = true;
                                }
                            }, timeout);

                            // check if all STORE_ACKs have been received or if a timeout occurred
                            while (Controller.waiting && !timeoutOccurred[0]) {
                                try {
                                    Thread.sleep(500); // sleep for 0.5 seconds
                                } catch (InterruptedException e) {
                                    // handle exception
                                }
                            }

                            // if a timeout occurred, send an error message to the client
                            if (timeoutOccurred[0]) {
                                System.out.println("ERROR_TIMEOUT");
                                Controller.waiting=false;
                            }

                        }
                        if(Controller.storeAcks.get() == R){
                            Controller.storeAcks.set(0);
                            System.out.println("Done");
                            out.println("STORE_COMPLETE");
                        }else{
                            Controller.fileLocations.remove(parts[1]);
                            Controller.fileSize.remove(parts[1]);
                            Controller.index.removeFile(parts[1]);
                        }
                    }
                    }

                }
                if (command.equals("STORE_ACK")) {
                    Controller.storeAcks.set(Controller.storeAcks.get()+1);
                    System.out.println("A DStore store complete");
                    if(Controller.storeAcks.get() == R){
                        System.out.println("All received");
                        Controller.index.setState(parts[1],IndexState.STORED);
                        Controller.waiting = false;
                    }

                }
                if (command.equals("LOAD")) {
                    System.out.println("Controller received LOAD message");
                    reloadCounter = 0;
                    if (Controller.dstoreList.size() < R) {
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                        System.out.println("ERROR_NOT_ENOUGH_DSTORES");
                    }else if(!Controller.fileLocations.containsKey(parts[1]) || !Controller.index.getState(parts[1]).equals(IndexState.STORED)){
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                        System.out.println("ERROR_FILE_DOES_NOT_EXIST");
                    }
                    else{
//                        synchronized (Controller.fileLocations) {
                            List<Integer> dstores =Controller.fileLocations.get(parts[1]);
                            out.println("LOAD_FROM" + " " + dstores.get(0) + " " + Controller.fileSize.get(parts[1]));
                            System.out.println("LOAD_FROM" + " " + dstores.get(0) + " " + Controller.fileSize.get(parts[1]));
//                        }
                    }
                }
                if (command.equals("RELOAD")) {
                    reloadCounter++;
                    List<Integer> dstores =Controller.fileLocations.get(parts[1]);
                    if(reloadCounter == dstores.size()){
                        out.println("ERROR_LOAD");
                        System.out.println("ERROR_LOAD");
                    }else{
                        out.println("LOAD_FROM" + " " + dstores.get(reloadCounter) + " " + Controller.fileSize.get(parts[1]));
                        System.out.println("LOAD_FROM" + " " + dstores.get(reloadCounter) + " " + Controller.fileSize.get(parts[1]));
                    }
                }
                if (command.equals("REMOVE")) {
                    synchronized (fileLock){
                    if (Controller.dstoreList.size() < R) {
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                        System.out.println("ERROR_NOT_ENOUGH_DSTORES");
                    }else if(!Controller.index.containsFile(parts[1]) || !Controller.index.getState(parts[1]).equals(IndexState.STORED)){
                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                        System.out.println("ERROR_FILE_DOES_NOT_EXIST");
                    }else {
                        Controller.index.setState(parts[1],IndexState.REMOVING);
                        synchronized (Controller.fileLocations) {
                            List<Integer> dstores = Controller.fileLocations.get(parts[1]);
                            for (Integer dstore : dstores) {
                                String temp = Controller.d2.get(dstore);
                                ControllerThread dstoreThread = Controller.dstoreThreadMap.get(temp);
                                if (dstoreThread != null) {
                                    dstoreThread.send("REMOVE " + parts[1]);
                                }
                            }
                        }

                        Controller.waiting=true;
                        while (Controller.waiting) {
                            final boolean[] timeoutOccurred = {false};
                                Timer timer = new Timer();
                                timer.schedule(new TimerTask() {
                                    @Override
                                    public void run() {
                                        timeoutOccurred[0] = true;
                                    }
                                }, timeout);

                                // check if all STORE_ACKs have been received or if a timeout occurred
                                while (Controller.waiting && !timeoutOccurred[0]) {
                                    try {
                                        Thread.sleep(500); // sleep for 0.5 seconds
                                    } catch (InterruptedException e) {
                                        // handle exception
                                    }
                                }
                                // if a timeout occurred, send an error message to the client
                                if (timeoutOccurred[0]) {
                                    System.out.println("ERROR_TIMEOUT");
                                    break;
                                }
                        }
                        if(Controller.removeAcks.get() == R){
                            System.out.println("All received");
                            Controller.removeAcks.set(0);
                            System.out.println("REMOVE_COMPLETE");
                            out.println("REMOVE_COMPLETE");
                        }

                    }
                    }
                }

                if (command.equals("REMOVE_ACK")) {
                    Controller.removeAcks.set(Controller.removeAcks.get()+1);
//                    Controller.removeAcks.set(Controller.removeAcks.get()-1);
                    System.out.println("A DStore file remove complete" + parts[1]);
//                    System.out.println("ACKS:" + Controller.removeAcks);
                    synchronized (Controller.fileLocations){
                        List<Integer> dstores = Controller.fileLocations.get(parts[1]);
                        if(dstores.contains(portno)){
//                            dstores.remove(portno);
                            dstores.removeAll(Collections.singleton(portno));
                            Controller.fileLocations.remove(parts[1]);
                            Controller.fileLocations.put(parts[1],dstores);
                        }else{
                        }

                        if(Controller.fileLocations.get(parts[1]).size() == 0){
                            Controller.index.removeFile(parts[1]);
                            System.out.println("All removes received");
                            Controller.fileLocations.remove(parts[1]);
                            Controller.index.removeFile(parts[1]);
                            //                        Controller.removeAcks.set(0);
                            Controller.waiting = false;

                        }else{
                            System.out.println("More ACKS required");
                        }
                    }
                }

                else {
                    System.out.println("Controller received unrecognized message:" + message);
                }

            }
            if(isDstore){
                String temp = Controller.d2.get(portno);
                Controller.dstoreList.remove(temp);
                Controller.dstoreStatus.remove(temp);
                Controller.d2.remove(portno);
                Controller.noOfFiles.remove(portno);
                synchronized (Controller.fileLocations){
                    for(String file: Controller.fileLocations.keySet()){
                        List<Integer> dstores = Controller.fileLocations.get(file);
                        if(dstores.contains(portno)){
//                            dstores.remove(portno);
                            dstores.removeAll(Collections.singleton(portno));
                            Controller.fileLocations.remove(file);
                            Controller.fileLocations.put(file,dstores);
                        }
                    }
                }
            }
            Thread.currentThread().interrupt();

        } catch(IOException e){
            e.printStackTrace();}
    }
    public void send(String message){
        System.out.println(message);
        out.println(message);
    }
}


