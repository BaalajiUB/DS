package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;
import android.widget.ToggleButton;

public class SimpleDhtProvider extends ContentProvider {

    private DBHelper dbh;
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    public static final String TABLE_NAME = "Messages";
    private static final String key = "key";
    private static final String value = "value";

    static String REMOTE_PORTS = "";
    static final int SERVER_PORT = 10000;

    String Predecessor = "";
    String Successor = "";

    static String NEW_NODE_ENTRY = "NEW_NODE_ENTRY";
    static String NEW_NODE = "NEW_NODE";
    static String INSERT = "INSERT";
    static String QUERY = "QUERY";
    static String QUERY_SINGLE = "QUERY_SINGLE";
    static String DELETE = "DELETE";
    static String DELETE_SINGLE = "DELETE_SINGLE";
    static String REHASHING = "REHASHING";

    static final String TAG = "SimpleDhtProvider";

    public String myPort;

    static final String COORD_PORT = "11108";

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        dbh = new DBHelper(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPort = String.valueOf(Integer.parseInt(portStr));

        Log.d("Provider_"+myPort, "DBHelper created");
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.d("SERVER_"+myPort, "Can't create a ServerSocket");
            return false;
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,NEW_NODE_ENTRY ,myPort);
        return dbh != null;

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override

        protected Void doInBackground(String... msgs) {

            String TAG = NEW_NODE_ENTRY+"_"+myPort;
            String msgToSend = myPort;
            Log.i(TAG, "Message to send : " + msgToSend);

            Socket socket;
            if (null != msgToSend || !msgToSend.isEmpty()) {
                //Client logic

                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(COORD_PORT));
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                    Thread.sleep(10);
                    out.writeUTF(NEW_NODE_ENTRY + ":" + msgToSend + " ");
                    out.flush();

                    Log.d(TAG, TAG + ", Message written : " + msgToSend);
                    out.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG,"Exception");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String TAG = "Insert";
        // TODO Auto-generated method stub
        String KEY_I = values.getAsString(key);
        String VALUE_I = values.getAsString(value);

        //Log.d(TAG, myPort + " : " + KEY_I + " , " + VALUE_I);

        String hash_key = "";
        String pred_hash = "";
        String my_hash = "";
        try {
            hash_key = genHash(KEY_I);
            pred_hash = genHash(Predecessor);
            my_hash = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String first_port = REMOTE_PORTS.split(",")[0].trim();
        Log.d(TAG,"FIRST PORT : " + first_port);
        boolean local = false;

        if(REMOTE_PORTS.split(",").length <= 1){
            Log.d(TAG, "Insert condition : Single insert");
            local = true;
        }

        else {
            if (!myPort.equals(first_port)) {
                if (hash_key.compareTo(pred_hash) > 0 && hash_key.compareTo(my_hash) <= 0) {
                    Log.d(TAG, "Insert condition : !PORT1 insert");
                    local = true;
                }
            } else {
                if (hash_key.compareTo(pred_hash) > 0 || hash_key.compareTo(my_hash) <= 0) {  //changed from == to <= on condition 2
                    Log.d(TAG, "Insert condition : PORT1 insert");
                    local = true;
                }
            }
        }

        if (local){ //if range in local, insert
            SQLiteDatabase db = dbh.getWritableDatabase();
            long newRowId = db.replace(DBHelper.TABLE_NAME, null, values); //insert won't replace
            Uri resultUri = ContentUris.withAppendedId(uri, newRowId);
            //Log.d(TAG, values.toString());
            Log.d(TAG+"ed", myPort + " : " + KEY_I + " , " + VALUE_I);

            //Log.d(TAG, resultUri.toString());
            if (newRowId == -1) {
                Log.v(TAG, "Insertion failed");
            }

            return resultUri;

        }
        else{
        //send the message to successor
        Log.d(TAG, "Insert condition : successor insert > Successor = " + Successor + " > " + KEY_I);

        Socket succ_socket = null;
        try {
            succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor)*2);
            DataOutputStream succ_out = null;
            succ_out = new DataOutputStream(succ_socket.getOutputStream());
            Thread.sleep(10);
            succ_out.writeUTF(INSERT + ":" + KEY_I + "," + VALUE_I);
            succ_out.flush();
            Log.d(TAG, KEY_I + "," + VALUE_I + " sent to > " + Successor);
            succ_out.close();
            succ_socket.close();

        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG,"Exception");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        }

    return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = dbh.getReadableDatabase();
        Cursor cursor = null;

        if(selection.equals("@")){
            cursor =  db.rawQuery("select * from " + TABLE_NAME, null);
        }

        else if(selection.equals("*")){
            //cursor = db.rawQuery("select * from " + TABLE_NAME,null);
            //initiate by sending message to successor of initiator

            Log.d(TAG, "Query *");

            Socket succ_socket = null;
            try {
                succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myPort)*2);
                DataOutputStream succ_out = null;
                DataInputStream succ_in = null;

                //request to self server
                succ_out = new DataOutputStream(succ_socket.getOutputStream());
                Thread.sleep(10);
                succ_out.writeUTF(QUERY + ":" + String.valueOf(REMOTE_PORTS.split(",").length) + " ");
                succ_out.flush();
                Log.d(TAG,"Request sent > " + QUERY + ":" + String.valueOf(REMOTE_PORTS.split(",").length) + "to self " + myPort);

                succ_in = new DataInputStream(succ_socket.getInputStream());
                String inp_msg = succ_in.readUTF();
                if (inp_msg != null) {
                    //string to cursor
                    Log.d(TAG,"Message recieved from self > " + inp_msg);
                    Log.d(TAG,"Length of recieced message : " + inp_msg.split(",").length);
                    cursor = getCursorFromList(inp_msg);
                    Log.d(TAG,"Cursor size > " + cursor.getCount());
                }
                succ_socket.close();
            } catch (IOException e) {
                e.printStackTrace();
                Log.e("Query * call","Exception");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        else if(selection.equals("<=")){
            try {
                cursor = db.rawQuery("select * from " + TABLE_NAME + " where key <= \'" + genHash(Predecessor) + "\'",null);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        else if(sortOrder != null){
            cursor = db.query(
                    DBHelper.TABLE_NAME,   // The table to query
                    null,             // The array of columns to return (pass null to get all)
                    "key='" + selection + "'",              // The columns for the WHERE clause
                    selectionArgs,          // The values for the WHERE clause
                    null,                   // don't group the rows
                    null,                   // don't filter by row groups
                    null               // The sort order
            );
        }
        else{
            Socket succ_socket = null;
            try {
                succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myPort)*2);
                DataOutputStream succ_out = null;
                DataInputStream succ_in = null;

                //request to self server
                succ_out = new DataOutputStream(succ_socket.getOutputStream());
                Thread.sleep(10);
                succ_out.writeUTF(QUERY_SINGLE + ":" + selection + " ");
                succ_out.flush();
                Log.d(TAG,"Request sent > " + QUERY_SINGLE + ":" + selection);

                succ_in = new DataInputStream(succ_socket.getInputStream());
                String inp_msg = succ_in.readUTF();
                if (inp_msg != null) {
                    //string to cursor
                    Log.d(TAG,"Message recieved > " + inp_msg);
                    cursor = getCursorFromList(inp_msg);
                    Log.d(TAG,"Cursor size > " + cursor.getCount());
                }
                succ_socket.close();
            } catch (IOException e) {
                e.printStackTrace();
                Log.e("Query * call","Exception");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        return cursor;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = dbh.getWritableDatabase();
        int row_count = 0;
        if(selection.equals("@")){
            //db.execSQL("delete from " + TABLE_NAME);

            //https://stackoverflow.com/questions/9599741/how-to-delete-all-record-from-table-in-sqlite-with-android
            row_count = db.delete(TABLE_NAME, "1", null);
        }
        else if(selection.equals("*")){
            //db.execSQL("delete from " + TABLE_NAME);

            //row_count = db.delete(TABLE_NAME, "1", null);

            //call delete of successor

            //String[] temp = REMOTE_PORTS.split(",");

            Socket succ_socket = null;
            try {
                succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myPort)*2);
                DataOutputStream succ_out = null;
                DataInputStream succ_in = null;

                succ_out = new DataOutputStream(succ_socket.getOutputStream());
                Thread.sleep(10);
                succ_out.writeUTF(DELETE + ":" + REMOTE_PORTS.length() + " ");
                succ_out.flush();

                String inp_msg = succ_in.readUTF();
                if(inp_msg!=null){
                    row_count = Integer.parseInt(inp_msg);
                }

            } catch (IOException e) {
                e.printStackTrace();
                Log.e("Delete * call","Exception");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else if(selection.equals("<=")){
            try {
                //db.execSQL("delete from " + TABLE_NAME + " where key <= \"" + genHash(Predecessor) + "\"");

                //https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android
                row_count = db.delete(TABLE_NAME, key + " <= '" + genHash(Predecessor) + "'", null);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        else if(selectionArgs!=null){
            String TAG = DELETE_SINGLE;
            row_count = db.delete(TABLE_NAME, key + " = '" + selection + "'", null);
            Log.d(TAG,"Delete count in AVD " + myPort + " > " + row_count);
            Log.d("Delete", "Delete count > " + row_count);
            return row_count;
        }

        else{
            String TAG = DELETE_SINGLE;
            //row_count = db.delete(TABLE_NAME, key + " = '" + selection + "'", null);
            //Log.d(TAG,"Delete count in AVD " + myPort + " > " + row_count);
            //if (row_count == 0){
            Socket succ_socket = null;
            try {
                    //Sending to self
                    succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myPort)*2);
                    DataOutputStream succ_out = null;
                    DataInputStream succ_in = null;
                    Log.d(TAG, "Sending delete request to self > " + myPort);

                    succ_out = new DataOutputStream(succ_socket.getOutputStream());
                    Thread.sleep(10);
                    succ_out.writeUTF(DELETE_SINGLE + ":" + selection + "," + myPort);
                    succ_out.flush();

                    Log.d(TAG,"Message sent to successor > " + DELETE_SINGLE + ":" + selection + "," + myPort);

                    succ_in = new DataInputStream(succ_socket.getInputStream());
                    String inp_msg = succ_in.readUTF();
                    if(inp_msg!=null){
                        Log.d(TAG, "Message from successor > " + row_count);
                        inp_msg = inp_msg.trim();
                        row_count = Integer.parseInt(inp_msg);
                    }

            } catch (IOException e) {
                e.printStackTrace();
                Log.e("Delete single","Exception");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            }

        Log.d("Delete", "Delete count > " + row_count);
        return row_count;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.d("SERVER_"+myPort, "Serversocket created");
            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    //DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    String msgRecieved = in.readUTF();

                    if (null != msgRecieved) {
                        Log.d("SERVER_"+myPort,"Socket Accepted");
                        //Log.d("SERVER_"+myPort,msgRecieved);
                        String[] msgs = msgRecieved.split(":");
                        Log.d("SERVER_REC" , msgRecieved);
                        String TAG_input = msgs[0].trim();
                        String payload = msgs[1].trim();

                        //NEW NODE ENTRY at 5554
                        if (TAG_input.equals(NEW_NODE_ENTRY)) {

                            String TAG = NEW_NODE_ENTRY;
                            Log.d(TAG, "Payload received : " + payload);

                            Log.d(TAG,"Ports list before : " + REMOTE_PORTS);

                            //add the node to REMOTE PORTS list;
                            if (!REMOTE_PORTS.equals("")) {
                                REMOTE_PORTS += ",";
                            } //add "," if not the first port in list
                            REMOTE_PORTS += payload;

                            //Log.d(TAG,REMOTE_PORTS);
                            Log.d(TAG,"Ports list after : " + REMOTE_PORTS);

                            //sort the REMOTE_PORT list
                            REMOTE_PORTS = sort_ports(REMOTE_PORTS);
                            Log.d(TAG,"Ports list sorted : " + REMOTE_PORTS);


                            Log.d(TAG, "Predecessor, Successor : " + Predecessor+","+Successor);
                            //update predecessor
                            //update successor
                            String[] ports_ar = REMOTE_PORTS.split(",");
                            String[] ports_arr = new String[ports_ar.length];
                            for(int i=0; i<ports_ar.length; i++){
                                ports_arr[i] = ports_ar[i].trim();
                            }
                            if(ports_arr.length > 1){
                                int my_ind = Arrays.asList(ports_arr).indexOf(myPort);
                                int len = ports_arr.length;
                                int pred_ind = (my_ind-1)%len;
                                pred_ind = pred_ind>=0? pred_ind : len + pred_ind;
                                int succ_ind = (my_ind+1)%len;
                                Predecessor = ports_arr[pred_ind];
                                Successor = ports_arr[succ_ind];
                            }
                            Log.d(TAG, "Predecessor, Successor : " + Predecessor+","+Successor +" myport: "+myPort);

                            String first_port = "5554";

                            //send the new node to successor
                            if (!Successor.equals("")){ // && !Successor.equals(first_port)) {
                                Log.d(TAG,"Sending to Successor");
                                Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor)*2);
                                DataOutputStream succ_out = null;
                                succ_out = new DataOutputStream(succ_socket.getOutputStream());
                                Thread.sleep(10);
                                succ_out.writeUTF(NEW_NODE + ":" + REMOTE_PORTS+ " ");
                                succ_out.flush();
                                Log.d(TAG,NEW_NODE + ":" + REMOTE_PORTS);
                                succ_out.close();
                                succ_socket.close();
                            }
                        }

                        else if (TAG_input.equals(NEW_NODE)) {
                            //update remote ports
                            String TAG = NEW_NODE;
                            REMOTE_PORTS = payload;
                            Log.d(TAG, "Updated REMOTE_PORTS > " + REMOTE_PORTS);

                            String[] ports_ar = REMOTE_PORTS.split(",");
                            String[] ports_arr = new String[ports_ar.length];
                            for(int i=0; i<ports_ar.length; i++){
                                ports_arr[i] = ports_ar[i].trim();
                            }

                            //update predecessor
                            //update successor
                            if(ports_arr.length > 1){
                                int my_ind = Arrays.asList(ports_arr).indexOf(myPort);
                                int len = ports_arr.length;
                                int pred_ind = (my_ind-1)%len;
                                pred_ind = pred_ind>=0? pred_ind : len + pred_ind;
                                int succ_ind = (my_ind+1)%len;
                                String old_Predecessor = Predecessor;
                                //String old_Successor = Successor;
                                Predecessor = ports_arr[pred_ind];
                                Successor = ports_arr[succ_ind];

                                Log.d(TAG, "Updated Predecessor, Successor > " + Predecessor + "," + Successor+" myport: "+myPort);

                                //repartition chord data
                                if (!Predecessor.equals(old_Predecessor)){
                                    //get all rows to be removed into new node
                                    Cursor cursor = query(mUri,null,"<=",null,null);

                                    //send cursor data to predecessor
                                    Socket pred_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Predecessor)*2);
                                    DataOutputStream pred_out = null;
                                    pred_out = new DataOutputStream(pred_socket.getOutputStream());

                                    while(cursor.moveToNext()) {
                                        String KEY_I = cursor.getString(0);
                                        String VALUE_I = cursor.getString(1);
                                        Thread.sleep(10);
                                        pred_out.writeUTF(REHASHING + ":" + KEY_I + "," + VALUE_I);
                                        pred_out.flush();

                                    }
                                    pred_out.close();
                                    pred_socket.close();

                                    //remove all keys <= new node key
                                    delete(mUri,"<=",null);

                                    Log.d(TAG, "Repartitioning complete");
                                }
                            }


                            String first_port = "5554";

                            if (Successor!="" && !Successor.equals(first_port)) {

                                Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor)*2);
                                DataOutputStream succ_out = null;
                                succ_out = new DataOutputStream(succ_socket.getOutputStream());
                                Thread.sleep(10);
                                succ_out.writeUTF(NEW_NODE + ":" + REMOTE_PORTS +" ");
                                succ_out.flush();
                                succ_out.close();
                                succ_socket.close();
                            }
                        }

                        else if(TAG_input.equals(REHASHING)){
                            String KEY_I = msgs[1];
                            String VALUE_I = msgs[2];

                            ContentValues cv = new ContentValues();
                            cv.put(key, KEY_I);
                            cv.put(value, VALUE_I);
                            Uri t_uri = insert(mUri,cv);

                        }

                        else if(TAG_input.equals(DELETE)){

                            int count = delete(mUri,"@",null);

                            if(Integer.parseInt(payload) > 1) {

                                Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor)*2);
                                DataOutputStream succ_out = null;
                                DataInputStream succ_in = null;

                                //request to successor
                                succ_out = new DataOutputStream(succ_socket.getOutputStream());
                                Thread.sleep(10);
                                succ_out.writeUTF(DELETE + ":" + String.valueOf(Integer.parseInt(payload) - 1) + " ");

                                //succ_out.writeUTF(QUERY + ":" + String.valueOf(Integer.parseInt(payload) - 1));
                                succ_out.flush();

                                String inp_msg = succ_in.readUTF();
                                if (inp_msg != null) {
                                    count += Integer.parseInt(inp_msg);

                                    //return msg_load to predecessor
                                }
                                //succ_out.close();
                                succ_socket.close();
                            }
                            DataOutputStream pred_out = new DataOutputStream(socket.getOutputStream());
                            Thread.sleep(10);
                            pred_out.writeUTF(Integer.toString(count) + " ");
                            pred_out.flush();

                        }

                        else if(TAG_input.equals(QUERY)){
                                Cursor cursor_local =  query(mUri, null, "@",null,null);
                                String msg_load = cursor_to_string(cursor_local);
                                msg_load = msg_load.trim();
                                Log.d(TAG,"Mesasge from local AVD : " + msg_load);
                            if(Integer.parseInt(payload) > 1) {

                                Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor)*2);
                                DataOutputStream succ_out = null;
                                DataInputStream succ_in = null;

                                //request to successor
                                succ_in = new DataInputStream(succ_socket.getInputStream());
                                succ_out = new DataOutputStream(succ_socket.getOutputStream());
                                Thread.sleep(10);
                                succ_out.writeUTF(QUERY + ":" + String.valueOf(Integer.parseInt(payload) - 1) + " ");
                                succ_out.flush();

                                Log.d(TAG,"Requesting from successor : " + Successor);

                                String inp_msg = succ_in.readUTF();
                                if (inp_msg != null) {
                                    inp_msg = inp_msg.trim();
                                    Log.d(TAG,"Mesasge from successor AVD : " + inp_msg);
                                    //if (msg_load.trim().equals("") && !inp_msg.trim().equals("")){msg_load+=",";}
                                    msg_load += "," + inp_msg;
                                    msg_load = msg_load.trim();
                                    if(msg_load.equals(",")){msg_load = "";}
                                    else {
                                        if (msg_load.charAt(0) == ',') {
                                            msg_load = msg_load.substring(1, msg_load.length());
                                        }
                                        if (msg_load.charAt(msg_load.length() - 1) == ',') {
                                            msg_load = msg_load.substring(0, msg_load.length() - 1);
                                        }
                                    }
                                    //return msg_load to predecessor
                                }
                                //succ_out.close();
                                succ_socket.close();
                            }
                            msg_load = msg_load.trim();
                            Log.d(TAG, "Message to send > " + msg_load);
                            DataOutputStream pred_out = new DataOutputStream(socket.getOutputStream());
                            Thread.sleep(10);
                            pred_out.writeUTF(msg_load + " ");
                            pred_out.flush();
                        }

                        else if(TAG_input.equals(INSERT)){
                            ContentValues cv = new ContentValues();
                            String[] key_val = payload.split(",");
                            String KEY_I = key_val[0].trim();
                            String VALUE_I = key_val[1].trim();
                            cv.put(key,KEY_I);
                            cv.put(value,VALUE_I);
                            Uri t_uri = insert(mUri,cv);
                        }

                        else if(TAG_input.equals(QUERY_SINGLE)){
                            String TAG = QUERY_SINGLE;
                            String return_msg;
                            String KEY_I = payload.trim();
                            Cursor cursor = query(mUri,null,KEY_I,null,"KEY");
                            return_msg = cursor_to_string(cursor);

                            if(cursor.getCount() == 0){
                                Log.d(TAG,"Requesting successor > " + Successor);
                                Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor)*2);
                                DataOutputStream succ_out = null;
                                DataInputStream succ_in = null;

                                //request to successor
                                succ_out = new DataOutputStream(succ_socket.getOutputStream());
                                Thread.sleep(10);
                                succ_out.writeUTF(QUERY_SINGLE + ":" + KEY_I);
                                succ_out.flush();

                                succ_in = new DataInputStream(succ_socket.getInputStream());
                                String inp_msg = succ_in.readUTF();
                                Log.d(TAG,"Reply from successor > " + inp_msg);
                                if (inp_msg != null) {
                                    Log.d(TAG,"Message from Successor : " + inp_msg);
                                    return_msg = inp_msg;
                                    //return msg_load to predecessor
                                }
                                //succ_out.close();
                                succ_socket.close();
                            }
                            Log.d(TAG, "Message to send > " + return_msg + " to > "  + Predecessor);
                            DataOutputStream pred_out = new DataOutputStream(socket.getOutputStream());
                            Thread.sleep(100);
                            pred_out.writeUTF(return_msg  + " ");
                            pred_out.flush();
                        }

                        else if(TAG_input.equals(DELETE_SINGLE)){
                            String[] inp_lst = payload.split(",");
                            String KEY_I = inp_lst[0].trim();
                            String succ_not = inp_lst[1].trim();
                            String[] selectionArgs = new String[1];
                            selectionArgs[0] =  "DUMMY";
                            int row_count = delete(mUri,KEY_I,selectionArgs);

                            if(row_count == 0 && !Successor.equals(succ_not)){
                                Socket succ_socket = null;
                                try {
                                    //Sending to self
                                    succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor)*2);
                                    DataOutputStream succ_out = null;
                                    DataInputStream succ_in = null;
                                    Log.d(TAG, "Sending delete request to self > " + myPort);

                                    succ_out = new DataOutputStream(succ_socket.getOutputStream());
                                    Thread.sleep(10);
                                    succ_out.writeUTF(DELETE_SINGLE + ":" + payload);
                                    succ_out.flush();

                                    Log.d(TAG,"Message sent to successor > " + DELETE_SINGLE + ":" + payload);

                                    succ_in = new DataInputStream(succ_socket.getInputStream());
                                    String inp_msg = succ_in.readUTF();
                                    if(inp_msg!=null){
                                        Log.d(TAG, "Message from successor > " + row_count);
                                        inp_msg = inp_msg.trim();
                                        row_count = Integer.parseInt(inp_msg);
                                    }

                                } catch (IOException e) {
                                    e.printStackTrace();
                                    Log.e("Delete single","Exception");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            DataOutputStream pred_out = new DataOutputStream(socket.getOutputStream());
                            Thread.sleep(100);
                            pred_out.writeUTF(Integer.toString(row_count));//  + " ");
                            pred_out.flush();

                        }

                        in.close();
                        socket.close();
                    }
                } catch (IOException e) {
                    System.out.println(e);
                    Log.e("Server Socket","Exception");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String cursor_to_string(Cursor cursor) {
        String msg_pair_collection = "";
        while(cursor.moveToNext()){
            if(!msg_pair_collection.equals("")){
                msg_pair_collection+=",";
            }
            msg_pair_collection += cursor.getString(0) + "," + cursor.getString(1);
        }
        return msg_pair_collection;
    }

    public Cursor getCursorFromList(String input) {
        MatrixCursor cursor = new MatrixCursor(
                new String[] {key, value}
        );

        String[] vals = input.split(",");
        for (int i=0; i< vals.length; i+=2){
            cursor.newRow()
                    .add(key, vals[i].trim())
                    .add(value, vals[i+1].trim());
        }

        return cursor;
    }

    private String sort_ports(String remotePorts) {

        String[] ports_ar = REMOTE_PORTS.split(",");
        String[] ports = new String[ports_ar.length];
        for(int i=0; i<ports_ar.length; i++){
            ports[i] = ports_ar[i].trim();
        }

        //ArrayList<String> port_list = (ArrayList<String>) Arrays.asList(ports);
        ArrayList<String> port_list = new ArrayList<String>(Arrays.asList(ports));
        Collections.sort(port_list, new port_sort());
        String port_arr = port_list.toString();
        return port_arr.substring(1,port_arr.length()-1);

    }

    class port_sort implements Comparator<String> {
        @Override
        public int compare(String lhs, String rhs) {
            //https://www.baeldung.com/java-compare-strings
            String l_hash ;
            String r_hash ;
            try {
                l_hash = genHash(lhs.trim());
                r_hash = genHash(rhs.trim());
                if(l_hash.equals(r_hash))
                    return 0;
                else if(l_hash.compareTo(r_hash)<0)
                    return -1;
                else
                    return 1;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }
}