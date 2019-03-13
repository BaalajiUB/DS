package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity{
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    private final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    //private static int id = 0;
    private static final String key = "key";
    private static final String value = "value";

    private int MSG_COUNTER = 0;
    private int SEQUENCE = 0;
    private PriorityQueue<ArrayList<Object>> BUFFER_QUEUE;

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

        Log.i("OnCreate", "App alive");
        BUFFER_QUEUE = new PriorityQueue<ArrayList<Object>>(10, new CustomComparator());
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.d("SERVER","Serversocket created");
        } catch (IOException e) {
            Log.d("SERVER", "Can't create a ServerSocket");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
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
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, cur, myPort);
                Log.d("TV",cur);
            }
        });       /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
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
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    //Log.e(TAG, "Input stream for the socket is created. Status " + in.ready());
                    ArrayList<Object> msgRecieved = (ArrayList<Object>) in.readObject();
                    if (null != msgRecieved) {
                        Log.d("SERVER", "Message received : " + msgRecieved.toString());
                        SEQUENCE += 1;
                        //ArrayList<Object> input_list = new ArrayList<Object>(Arrays.asList(msgRecieved.split(",")));
                        if (msgRecieved.size() == 3) {   //<msg, MSG_COUNTER, MyPort>
                            Log.d("SERVER",msgRecieved + "\t 2");
                            msgRecieved.add(SEQUENCE);
                            out.writeObject(msgRecieved); //<MSG, MSG_ID, P_ID, SEQUENCE>
                            Log.d("SERVER",msgRecieved.toString() + "\t 3");
                            //adding the output message to priority queue
                            msgRecieved.add("NOT_FINAL");
                            BUFFER_QUEUE.add(msgRecieved); //<MSG, MSG_ID, P_ID, SEQUENCE, "NOT_FINAL">
                        }

                        else if (msgRecieved.size() == 4){   //<msg, MSG_COUNTER, MyPort, Final_sequence number>
                            //updating the priority queue
                            String INPUT_MSG = msgRecieved.get(0).toString().trim();
                            String INPUT_MSG_ID = msgRecieved.get(1).toString().trim();
                            String INPUT_PORT = msgRecieved.get(2).toString().trim();
                            Integer MSG_SEQ = Integer.parseInt(msgRecieved.get(3).toString());
                            int n = BUFFER_QUEUE.size();
                            int i = 0;
                            while(i < n){
                                ArrayList<Object> arr = BUFFER_QUEUE.poll();
                                //System.out.println(arr.toString());
                                if (arr.get(0).toString().trim() == INPUT_MSG && arr.get(1).toString().trim() == INPUT_MSG_ID && arr.get(2).toString().trim() == INPUT_PORT){
                                    arr.set(3, MSG_SEQ);
                                    arr.set(4,"FINAL");
                                }
                                BUFFER_QUEUE.add(arr);
                                i+=1;
                            }

                            //delivering all the FINAL messages in BUFFER
                            ArrayList<Object> arr = BUFFER_QUEUE.peek();
                            String STATUS = arr.get(4).toString().trim();
                            while(STATUS == "FINAL"){
                                ArrayList<Object> delivered = BUFFER_QUEUE.poll();
                                publishProgress(delivered.get(0).toString().trim(), delivered.get(3).toString().trim()); //message to display in UI
                                Log.i("DELIVERY", delivered.toString());

                                arr = BUFFER_QUEUE.peek();
                                STATUS = arr.get(4).toString().trim();

                            }
                        SEQUENCE = MSG_SEQ > SEQUENCE? MSG_SEQ : SEQUENCE; //Updating SEQUENCE
                        }
                        //publishProgress(msgRecieved);
                        //out.writeUTF("ACK");

                        //Log.d("SERVER", "ACK sent for message : " + msgRecieved);
                    }
                    else{Log.e("SERVER", "Message received in else part : " + msgRecieved);}

                    in.close();
                    out.flush();
                    out.close();
                    socket.close();

                } catch (IOException e) {
                    System.out.println(e);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(String... strings) {
            Log.e(TAG, "Message in onProgressUpdate : " + strings[0]);
            String strReceived = strings[0].trim();
            Integer ID = Integer.parseInt(strings[1].trim());
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append("\t\t\t" + strReceived + "\n");

            ContentValues cv = new ContentValues();
            //cv.put(key,Integer.toString(id));
            cv.put(key,ID.toString());
            cv.put(value,strReceived);
            Uri t_uri = getContentResolver().insert(mUri,cv);

            Log.d("URI",t_uri.toString());
            Log.d("insert",cv.toString());
            //id++;

            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String msgToSend = msgs[0].trim();
                int myPort = Integer.parseInt(msgs[1].trim());
                Log.i("CLIENT", "Message to send : " +msgToSend);

                ArrayList<Integer> sequence_list = new ArrayList<Integer>();
                int final_sequence_number = -1;

                if (null != msgToSend || !msgToSend.isEmpty()) {
                    ArrayList<Object> msg = new ArrayList<Object>();
                    msg.add(msgToSend);
                    MSG_COUNTER += 1;
                    msg.add(MSG_COUNTER);
                    msg.add(myPort);
                    for (String port : REMOTE_PORTS) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(msg); //<msg, MSG_COUNTER, MyPort>

                        Log.d("CLIENT",msg.toString() + "\t 1");

                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
/*
                        if (in.readUTF().trim().equals("ACK")){
                            Log.d("ACK","ACK received for " + msgToSend);
                            in.close();
                            out.flush();
                            out.close();
                            socket.close();
                        }
*/
                        ArrayList<Object> input_list = (ArrayList<Object>) in.readObject();
                        Log.d("CLIENT",input_list.toString() + "\t 4");
                        //ArrayList<Object> input_list = new ArrayList<Object>(Arrays.asList(input.split(","))); //<MSG, MSG_ID, P_ID, SEQUENCE>
                        Integer sequence_proposal = new Integer(String.valueOf(input_list.get(3)));
                        sequence_list.add(sequence_proposal);
                        if (sequence_list.size()==5){
                            final_sequence_number = Collections.max(sequence_list);
                            Log.d("CLIENT","All replies recieved" + "\t 5");
                        }
                        in.close();
                        out.flush();
                        out.close();
                        socket.close();
                    }
                }
                //multicasting <M,Sequence>
                ArrayList<Object> msg_seq = new ArrayList<Object>();
                msg_seq.add(msgToSend);
                msg_seq.add(MSG_COUNTER);
                msg_seq.add(myPort);
                msg_seq.add(final_sequence_number);
                for (String port : REMOTE_PORTS) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msg_seq); //<msg, MSG_COUNTER, MyPort, Final_sequence number>
                    Log.d("CLIENT",msg_seq.toString() + "\t 6");
                    out.flush();
                    out.close();
                    socket.close();
                }

                } catch (UnknownHostException e) {
                Log.e("CLIENT", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("CLIENT", "ClientTask socket IOException");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    class CustomComparator implements Comparator<ArrayList<Object>> {

        // Overriding compare()method of Comparator
        // for descending order of cgpa
        public int compare(ArrayList<Object> arr1, ArrayList<Object> arr2) {
            int i1 = Integer.parseInt(arr1.get(3).toString()), i2 = Integer.parseInt(arr2.get(3).toString());
            if (i1 > i2)
                return 1;
            else if (i1 < i2)
                return -1;
            return 0;
        }
    }

}
