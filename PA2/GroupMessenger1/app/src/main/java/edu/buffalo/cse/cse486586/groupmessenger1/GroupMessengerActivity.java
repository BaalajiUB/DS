package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import org.w3c.dom.Text;

import java.io.BufferedReader;
import java.io.DataInput;
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

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    private final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
    private static int id = 0;
    private static final String key = "key";
    private static final String value = "value";

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
//        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
//        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
//        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.d("SERVER","Serversocket created");
        } catch (IOException e) {
            Log.d("SERVER", "Can't create a ServerSocket");
            return;
        }
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText et = (EditText) findViewById(R.id.editText1);
                String cur = et.getText().toString();

                et.setText("");

                TextView tv = (TextView) findViewById(R.id.textView1);
                tv.append(cur + "\n");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, cur, "");
                Log.d("TV",cur);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            //Log.d(TAG, "Entered server class");
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    //Log.e(TAG, "Input stream for the socket is created. Status " + in.ready());
                    String msgRecieved = in.readUTF();
                    if (null != msgRecieved) {
                        Log.d("SERVER", "Message received : " + msgRecieved);
                        publishProgress(msgRecieved);
                        out.writeUTF("ACK");

                        Log.d("SERVER", "ACK sent for message : " + msgRecieved);
                    }
                        in.close();
                        out.flush();
                        out.close();
                        socket.close();
                        Log.e(TAG, "Message received in else part : " + msgRecieved);

                } catch (IOException e) {
                    System.out.println(e);
                }
            }
        }

        protected void onProgressUpdate(String... strings) {
            Log.e(TAG, "Message in onProgressUpdate : " + strings[0]);
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append("\t\t\t" + strReceived + "\n");

            ContentValues cv = new ContentValues();
            cv.put(key,Integer.toString(id));
            cv.put(value,strReceived);
            Uri t_uri = getContentResolver().insert(mUri,cv);

            Log.d("URI",t_uri.toString());
            Log.d("insert",cv.toString());
            id++;

            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String msgToSend = msgs[0].trim();
                Log.i(TAG, "Message to send : " +msgToSend);

                if (null != msgToSend || !msgToSend.isEmpty()) {
                    for (String port : REMOTE_PORTS) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF(msgToSend);

                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        if (in.readUTF().trim().equals("ACK")){
                            Log.d("ACK","ACK received for " + msgToSend);
                            in.close();
                            out.flush();
                            out.close();
                            socket.close();
                        }
                    }
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

}