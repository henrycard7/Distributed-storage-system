import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;


public class Dstore {
    public static final Object filesLock = new Object();
    //    public static Object filesLock;
    public static List<DstoreThread> dstoreThreads = new ArrayList<DstoreThread>();
    public static  Socket controllerSocket;

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]); // Port number for Dstore
        int cport = Integer.parseInt(args[1]); // Port number for controller
        int timeout = Integer.parseInt(args[2]); // Timeout value
        String file_folder = args[3]; // Folder to store files


        // Connect to the controller
        System.out.println("Dstore connecting to controller on port " + cport);
        InetAddress address = InetAddress.getLocalHost();
        Socket controllerSocket = new Socket(address, cport);
        Dstore.controllerSocket = controllerSocket;
        System.out.println("after controller socket test");
//        String filePath =System.getProperty("user.dir") + file_folder ;


//        // Empty the file folder
//        File directory = new File(filePath);
//        File[] files = directory.listFiles();
//        if(files!=null) {
//            for(File f: files) {
//                f.delete();
//            }
//        }



        // Create a server socket
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("DStore listening on port " + port);




        try {
            // Get input/output streams for communication with the controller
//            BufferedReader in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);

            // Send a join request to the controller

            out.println("JOIN " + port);
            System.out.println("JOIN " + port);


//            // Wait for a response from the controller
//            String response = in.readLine();
//            System.out.println("Dstore received response from controller: " + response);


        } catch (IOException e) {
            e.printStackTrace();
        }
        // Create a new thread to handle communication with the controller
        Thread thread = new Thread(new DstoreThread(controllerSocket, timeout, port, cport, file_folder));
        thread.start();

        while (true) {
            // Wait for a connection from a client
            Socket clientSocket = serverSocket.accept();
            String dstoreEndpoint = clientSocket.getRemoteSocketAddress().toString();
            System.out.println("Controller received connection from client at " + dstoreEndpoint);

            // Create a new thread to handle communication with the client
            Thread thread2 = new Thread(new DstoreThread(clientSocket, timeout, port, cport, file_folder));
            thread2.start();
        }
    }
}

class DstoreThread implements Runnable {
    private static List<Integer> readyToSend= new ArrayList<Integer>();
    private static final Object fileLock = new Object();
    private int port;
    private Socket controllerSocket;
    private int timeout;
    private int cport;
    private String file_folder;
    private volatile Boolean waitingForContent = false;
    private File currentFile;
    private String currentFileName;
    InetAddress address = InetAddress.getLocalHost();
    private PrintWriter out;


    public DstoreThread(Socket controllerSocket, int timeout, int port, int cport, String file_folder) throws IOException {
        this.controllerSocket = controllerSocket;
        this.timeout = timeout;
        this.port = port;
        this.cport = cport;
        this.file_folder = file_folder;
        this.out  = new PrintWriter(controllerSocket.getOutputStream(), true);
    }

    @Override
    public void run() {
        System.out.println("Dstore thread " + Thread.currentThread().getId() + " started");
        Dstore.dstoreThreads.add(DstoreThread.this);


        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            InputStream inputStream = controllerSocket.getInputStream();
            while (true) {
                if(!waitingForContent){
                    String message = in.readLine();
                    System.out.println(message + " received");
                    String[] parts = message.split(" ");
                    String command = parts[0];
                    if (command.equals("STORE")) {
                        synchronized (fileLock) {
//                            File myFile = new File(System.getProperty("user.dir") + file_folder + File.separator + parts[1]);
                            File myFile = new File(file_folder + File.separator + parts[1]);
                            currentFile = myFile;
                            currentFileName = myFile.getName();
                            if (myFile.createNewFile()) {
                                System.out.println("File created: " + myFile.getName());
                            } else {
                                System.out.println("File already exists.");
                            }
                            Controller.fileSize.put(parts[1], parts[2]);
                            System.out.println(parts[1] + " " + parts[2]);
                            waitingForContent = true;
                            out.println("ACK");
                            System.out.println("ACK sent");
                        }
                    }

                    if(command.equals("LOAD_DATA")){
                        synchronized (fileLock){

                        try{
//
//                            FileInputStream input = new FileInputStream(System.getProperty("user.dir") + file_folder + File.separator + parts[1]);
                            FileInputStream input = new FileInputStream(file_folder + File.separator + parts[1]);
                            byte[] fileContent = input.readAllBytes();
                            OutputStream out2 = controllerSocket.getOutputStream();
                            out2.write(fileContent);
                            out2.close();
                        }catch (FileNotFoundException e){
                            controllerSocket.close();
                        }
                        }
                    }
                    if(command.equals("REMOVE")){
                        synchronized (fileLock){
                        try{

//                            String file =System.getProperty("user.dir") + file_folder +"\\" + parts[1];
//                            File myFile = new File(file_folder + File.separator + parts[1]);
                            File f= new File(file_folder + File.separator + parts[1]);
                            if(f.delete())
                            {
                                System.out.println(f.getName() + " deleted");
                                out.println("REMOVE_ACK" + " " + parts[1]);
                                System.out.println("REMOVE_ACK" + " " + parts[1]);
                            }
                            else
                            {
                                System.out.println("failed");
                                out.println("ERROR_FILE_DOES_NOT_EXIST" + " " + parts[1]);
                            }
                        }
                        catch(Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                    }
                    else {
                        System.out.println("DStore received unrecognized message:" + message);
                    }
                }
                if(waitingForContent){
                    synchronized (fileLock){
                        final boolean[] timeoutOccurred = {false};
                        Timer timer = new Timer();
                        timer.schedule(new TimerTask() {
                            @Override
                            public void run() {
                                waitingForContent = false;
                                timeoutOccurred[0] = true;
                            }
                        }, timeout);
                        int size = Integer.parseInt(Controller.fileSize.get(currentFileName));
                        System.out.println("Size: "+Integer.parseInt(Controller.fileSize.get(currentFileName)));
    //                    byte[] data = inputStream.readNBytes(Integer.parseInt(Controller.fileSize.get(currentFileName)));
                        byte[] data = new byte[size];
                        inputStream.readNBytes(data,0,size);
                        FileOutputStream f = new FileOutputStream(currentFile);
                        f.write(data);
                        System.out.println("Successfully wrote to the file.");
                        readyToSend.add(port);
                        synchronized (Dstore.dstoreThreads) {
                            for (DstoreThread dstore : Dstore.dstoreThreads) {
                                if (readyToSend.contains(dstore.port)){
                                    dstore.send("STORE_ACK" + " " + currentFileName);
                                }
                            }
                        }

                        waitingForContent = false;

                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void send(String message){
        if(this.controllerSocket==Dstore.controllerSocket){
            System.out.println(message);
            out.println(message);
        }
    }
}

