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
    static String DELETE = "DELETE";
    static String REHASHING = "REHASHING";


    public String myPort;

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
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.d("SERVER", "Serversocket created");
        } catch (IOException e) {
            Log.d("SERVER", "Can't create a ServerSocket");
            return false;
        }

//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,NEW_NODE_ENTRY ,myPort);

        String TAG = NEW_NODE_ENTRY;
        String msgToSend = myPort;
        Log.i(TAG, "Message to send : " + msgToSend);

        //Client logic
        String COORD_PORT = "11108";
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(COORD_PORT));
            DataOutputStream out = null;
            out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF(TAG + "," + msgToSend);
            out.flush();
            out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dbh != null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String TAG = "Insert";
        // TODO Auto-generated method stub
        SQLiteDatabase db = dbh.getWritableDatabase();
        long newRowId = db.replace(DBHelper.TABLE_NAME, null, values); //insert won't replace
        Uri resultUri = ContentUris.withAppendedId(uri, newRowId);
        Log.v(TAG, values.toString());
        Log.v(TAG, resultUri.toString());
        if (newRowId == -1) {
            Log.v(TAG, "Insertion failed");
        }

        return resultUri;
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
/*
        cursor = db.query(
                DBHelper.TABLE_NAME,   // The table to query
                projection,             // The array of columns to return (pass null to get all)
                "key='" + selection + "'",              // The columns for the WHERE clause
                selectionArgs,          // The values for the WHERE clause
                null,                   // don't group the rows
                null,                   // don't filter by row groups
                null               // The sort order
        );
*/
        if(selection.equals("@")){
            cursor =  db.rawQuery("select * from " + TABLE_NAME, null);
        }

        else if(selection.equals("*")){
            cursor = db.rawQuery("select * from " + TABLE_NAME,null);
            //send this cursor to successor till reached the source
        }

        else{
            try {
                cursor = db.rawQuery("select * from " + TABLE_NAME + "where key <= \"" + genHash(Predecessor) + "\"",null);
            } catch (NoSuchAlgorithmException e) {
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

            row_count = db.delete(TABLE_NAME, "1", null);
            //call delete of successor
        }
        else{
            try {
                //db.execSQL("delete from " + TABLE_NAME + " where key <= \"" + genHash(Predecessor) + "\"");

                //https://stackoverflow.com/questions/7510219/deleting-row-in-sqlite-in-android
                row_count = db.delete(TABLE_NAME, key + "<=" + genHash(Predecessor), null);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
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
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    //DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    String msgRecieved = in.readUTF();
                    in.close();
                    socket.close();
                    if (null != msgRecieved) {
                        String[] msgs = msgRecieved.split(",");

                        String TAG_input = msgs[0].trim();
                        String payload = msgs[1].trim();

                        //NEW NODE ENTRY at 5554
                        if (TAG_input.equals(NEW_NODE_ENTRY)) {

                            String TAG = NEW_NODE_ENTRY;
                            Log.d(TAG, "Message received : " + payload);

                            //add the node to REMOTE PORTS list;
                            if (!REMOTE_PORTS.equals("")) {
                                REMOTE_PORTS += ",";
                            } //add "," if not the first port in list
                            REMOTE_PORTS += NEW_NODE;

                            //sort the REMOTE_PORT list
                            REMOTE_PORTS = sort_ports(REMOTE_PORTS);

                            //update predecessor
                            //update successor
                            String[] ports_arr = REMOTE_PORTS.split(",");
                            if(ports_arr.length > 1){
                                int my_ind = Arrays.asList(ports_arr).indexOf(myPort);
                                int len = ports_arr.length;
                                int pred_ind = (my_ind-1)%len;
                                int succ_ind = (my_ind+1)%len;
                                Predecessor = ports_arr[pred_ind];
                                Successor = ports_arr[succ_ind];
                            }

                            String first_port = ports_arr[0];

                            //send the new node to successor
                            if (Successor!=null && !Successor.equals(first_port)) {
                                Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor));
                                DataOutputStream succ_out = null;
                                succ_out = new DataOutputStream(socket.getOutputStream());
                                succ_out.writeUTF(NEW_NODE + "," + REMOTE_PORTS);
                                succ_out.flush();
                                succ_out.close();
                                succ_socket.close();
                            }
                        }

                        else if (TAG_input.equals(NEW_NODE)) {

                            String[] ports_arr = REMOTE_PORTS.split(",");
                            String first_port = ports_arr[0];

                            if (Successor!=null && !Successor.equals(first_port)) {

                                //update remote ports
                                REMOTE_PORTS = payload;

                                //update predecessor
                                //update successor
                                if(ports_arr.length > 1){
                                    int my_ind = Arrays.asList(ports_arr).indexOf(myPort);
                                    int len = ports_arr.length;
                                    int pred_ind = (my_ind-1)%len;
                                    int succ_ind = (my_ind+1)%len;
                                    String old_Predecessor = Predecessor;
                                    String old_Successor = Successor;
                                    Predecessor = ports_arr[pred_ind];
                                    Successor = ports_arr[succ_ind];

                                    //repartition chord data
                                    if (!Predecessor.equals(old_Predecessor)){
                                        //get all rows to be removed into new node
                                        Cursor cursor = query(mUri,null,null,null,null);

                                        //send cursor data to predecessor
                                        Socket pred_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Predecessor));
                                        DataOutputStream pred_out = null;
                                        pred_out = new DataOutputStream(socket.getOutputStream());

                                        while(cursor.moveToNext()) {
                                            String KEY_I = cursor.getString(0);
                                            String VALUE_I = cursor.getString(1);

                                            pred_out.writeUTF(REHASHING + "," + KEY_I + "," + VALUE_I);
                                            pred_out.flush();

                                        }
                                        pred_out.close();
                                        pred_socket.close();

                                        //remove all keys <= new node key
                                        delete(mUri,"Conditional",null);

                                    }
                                }

                                Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor));
                                DataOutputStream succ_out = null;
                                succ_out = new DataOutputStream(socket.getOutputStream());
                                succ_out.writeUTF(NEW_NODE + "," + REMOTE_PORTS);
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
                    }
                } catch (IOException e) {
                    System.out.println(e);
                }
            }
        }
    }

    private String sort_ports(String remotePorts) {
        String[] ports = remotePorts.split(",");
        ArrayList<String> port_list = (ArrayList<String>) Arrays.asList(ports);
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