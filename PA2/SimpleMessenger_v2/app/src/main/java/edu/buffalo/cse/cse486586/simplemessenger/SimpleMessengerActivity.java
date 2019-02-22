package edu.buffalo.cse.cse486586.simplemessenger;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import edu.buffalo.cse.cse486586.simplemessenger.R;
import android.app.Activity;
import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.view.KeyEvent;
import android.view.View;
import android.view.View.OnKeyListener;
import android.widget.EditText;
import android.widget.TextView;
import javax.xml.transform.Result;

public class SimpleMessengerActivity extends Activity {
    static final String TAG = SimpleMessengerActivity.class.getSimpleName();
    static String[] REMOTE_PORTS = {"11108","11112","11116","11120","11124"};
    static final int SERVER_PORT = 10000;
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e(TAG,"Port number : " + myPort);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            //Log.e(TAG,"Server socket closed");
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        final EditText editText = (EditText) findViewById(R.id.edit_text);
        editText.setOnKeyListener(new OnKeyListener() {
            @Override
            public boolean onKey(View v, int keyCode, KeyEvent event) {
                if ((event.getAction() == KeyEvent.ACTION_DOWN) &&
                        (keyCode == KeyEvent.KEYCODE_ENTER)) {

                    String msg = editText.getText().toString() + "\n";
                    editText.setText(""); // This is one way to reset the input box.
                    TextView localTextView = (TextView) findViewById(R.id.local_text_display);
                    localTextView.append("\t" + msg); // This is one way to display a string.
                    TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
                    remoteTextView.append("\n");
                    Log.e(TAG,"Client socket creation for message " + msg);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

                    Log.e(TAG,"client socket closed for message " + msg);
                    return true;
                }
                return false;
            }
        });
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.d(TAG,"Entered server class");
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    //Log.e(TAG, "Connection request from client " + String.valueOf(socket.getRemoteSocketAddress()) + " accepted");
                    //https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    Log.e(TAG, "Input stream for the socket is created. Status " + in.ready());
                    String msgRecieved = in.readLine();
                    if (null != msgRecieved) {
                        Log.d(TAG, "Message received : " + msgRecieved);
                        publishProgress(msgRecieved);
                    } else {
                        in.close();
                        Log.e(TAG, "Message received in else part : " + msgRecieved);
                        break;
                    }
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            try{
                serverSocket.close();
            }catch(Exception ex) {System.out.println(ex);}
            return null;
        }
        protected void onProgressUpdate(String... strings) {

            Log.e(TAG,"Message in onProgressUpdate : "+strings[0]);
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.local_text_display);
            localTextView.append("\n");

            String filename = "SimpleMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;
            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        //Socket socket;
        @Override
        protected Void doInBackground(String... msgs) {
            try {
//                String remotePort = msgs[1];
//                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//                        Integer.parseInt(remotePort));
//                Log.d(TAG, "Socket status " + String.valueOf(socket.isConnected()));
//                Log.d(TAG, "My port : " + remotePort);
//                String msgToSend = msgs[0].trim(); //without trimming, it is sending an empty message which is treated as null.
//                Log.d(TAG, "client socket created for message " + msgToSend + " to port " + remotePort);
//                //https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
//                Log.e(TAG, "Message to send: " + msgToSend);
//                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//                //Log.e(TAG, "Output stream created to server for message " + msgToSend);
//                out.println(msgToSend);
//                //Log.e(TAG, "Message " + msgToSend + " sent");
//                //Log.e(TAG, "Client socket for message " + msgToSend + "closed");
//                out.flush();
//                out.close();
//                socket.close();


                String msgToSend = msgs[0].trim();
                Log.i(TAG, "ClientTask - doInBackground()" +msgToSend);
                /*
                 * TODO: Fill in your client code that sends out a message.
                 * Below modified code is based on understanding Oracle (2019), documentation (source code)
                 * - https://docs.oracle.com/javase/tutorial/networking/sockets/
                 */

                if (null != msgToSend || !msgToSend.isEmpty()) {
                    for (String port : REMOTE_PORTS) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(msgToSend);
                        /*BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        if(msgToSend.contains("ACK")){
                            out.close();
                            in.close();*/
                        out.flush();
                        Thread.sleep(30);
                        out.close();
                        socket.close();
                    }
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
/*
 ****************************
 *********References*********
 ****************************
 * //https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
 * */
