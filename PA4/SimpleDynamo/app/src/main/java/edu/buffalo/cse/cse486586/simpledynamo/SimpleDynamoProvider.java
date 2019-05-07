package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import java.util.HashMap;

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

public class SimpleDynamoProvider extends ContentProvider {

	private DBHelper dbh;
	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	public static final String TABLE_NAME = "Messages";
	private static final String key = "key";
	private static final String value = "value";

	static String REMOTE_PORTS = "";
	static final int SERVER_PORT = 10000;

	String Predecessor = "";
	String Successor = "";

	HashMap<String, String> map = new HashMap<String, String>();

	static String NEW_NODE_ENTRY = "NEW_NODE_ENTRY";
	static String NEW_NODE = "NEW_NODE";
	static String INSERT = "INSERT";
	static String QUERY = "QUERY";
	static String QUERY_SINGLE = "QUERY_SINGLE";
	static String DELETE = "DELETE";
	static String DELETE_SINGLE = "DELETE_SINGLE";
	static String RECOVERY = "RECOVERY";
	static String GET_LOCAL ="GET_LOCAL" ;
	static String GET_REPLICA = "GET_REPLICA";

	static final String TAG = "SimpleDynamo";

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
		Log.d("ENTRY", myPort);
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

			//REMOTE_PORTS = "5562, 5556, 5554, 5558, 5560";
			String TAG = NEW_NODE_ENTRY+"_"+myPort;
			String msgToSend = myPort;
			Log.i(TAG, "Message to send : " + msgToSend);

			if (null != msgToSend || !msgToSend.isEmpty()) {
				//Client logic
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(COORD_PORT));
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
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		insert_helper(uri,values);
		return null;
	}

	public void insert_helper(Uri uri, ContentValues values){
		String TAG = INSERT;
		Log.d("INSERT CHECK:", REMOTE_PORTS);
		//find bucket in which the data is to be inserted
		ArrayList<String> hashed_ports = new ArrayList<String>(5);
		int coordinator = -1;
		String KEY_I = values.getAsString(key);
		String VALUE_I = values.getAsString(value);
		REMOTE_PORTS = "5562, 5556, 5554, 5558, 5560";

		try {
			String hash_key = genHash(KEY_I);
			String[] nodes = REMOTE_PORTS.trim().split(",");
			for (String s : nodes) {
				hashed_ports.add(genHash(s.trim()));
			}
			Log.d("CHECK HASH", Arrays.toString(hashed_ports.toArray()));

			if (hash_key.compareTo(hashed_ports.get(0)) <= 0) {
				coordinator = 0;
			} else if (hash_key.compareTo(hashed_ports.get(1)) <= 0) {
				coordinator = 1;
			} else if (hash_key.compareTo(hashed_ports.get(2)) <= 0) {
				coordinator = 2;
			} else if (hash_key.compareTo(hashed_ports.get(3)) <= 0) {
				coordinator = 3;
			} else if (hash_key.compareTo(hashed_ports.get(4)) <= 0) {
				coordinator = 4;
			} else {
				coordinator = 0;
			}

			//get list of ports to which the data is to be inserted

			//String[] type = {"I", "R", "R"};
			String[] Remote_Port_List = REMOTE_PORTS.split(",");
			String coord_port = Remote_Port_List[coordinator].trim();
			ArrayList<String> lst = new ArrayList<String>();

			for (int i = 0; i < 3; i++) {
				int ind = (coordinator + i) % 5;
				String port = Remote_Port_List[ind].trim();
				Log.d("CHECK PORT: ", port);
				lst.add(port);

				Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port) * 2);
				DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());

				Thread.sleep(10);

				succ_out.writeUTF(INSERT + ":" + KEY_I + "," + VALUE_I + "," + coord_port + " ");
				succ_out.flush();
				Log.d(TAG, KEY_I + "," + VALUE_I + "," + coord_port + " sent to > " + port);

				//succ_out.close();
				succ_socket.close();

			}
			Log.d(KEY_I, "MESSAGE " + KEY_I + " : " + hash_key + " bucketed in : " + lst.get(0) + ", " + lst.get(1) + ", " + lst.get(2));

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
			Log.e(TAG,"Exception");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

	}

	public Uri insert(Uri uri, ContentValues values, String type) { //REPLICATION = Local Insertion

		String TAG = INSERT;
		String KEY_I = values.getAsString(key);
		String VALUE_I = values.getAsString(value);

		SQLiteDatabase db = dbh.getWritableDatabase();
		long newRowId = db.replace(DBHelper.TABLE_NAME, null, values); //insert won't replace
		Uri resultUri = ContentUris.withAppendedId(uri, newRowId);
		Log.d(TAG+"ed", myPort + " : " + KEY_I + " , " + VALUE_I);

		if (newRowId == -1) {
			Log.v(TAG, "Insertion failed");
		}
		return resultUri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		SQLiteDatabase db = dbh.getReadableDatabase();
		Cursor cursor = null;

		if(selection.equals("@")){
			cursor =  db.rawQuery("select * from " + TABLE_NAME, null);
		}

		else if(selection.equals("*")) {
			Log.d(TAG, "Query *");

			ArrayList<String> result = new ArrayList<String>();
			String[] remote_port_list = REMOTE_PORTS.split(",");
			for (String port : remote_port_list) {
				port = port.trim();
				try {
					Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port) * 2);

					DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());
					Thread.sleep(10);
					succ_out.writeUTF(QUERY + ":" + "*" + " ");
					succ_out.flush();
					Log.d(TAG, "Request sent > " + QUERY + ":" + "*");

					DataInputStream succ_in = new DataInputStream(succ_socket.getInputStream());
					String inp_msg = succ_in.readUTF();
					if (inp_msg != null) {
						Log.d(TAG, "Message recieved from self > " + inp_msg);
						Log.d(TAG, "Length of recieced message : " + inp_msg.split(",").length);
						result.add(inp_msg);
					}

					succ_out.close();
					succ_in.close();
					succ_socket.close();
				} catch (IOException e) {
					e.printStackTrace();
					Log.e("Query * call", "Exception");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			String rs = "";
			for(String inp : result){
				if(inp.trim().length()!=0){
					if(rs.compareTo("")!=0){rs+=",";}
					rs += inp.trim();
				}
			}
			rs = rs.trim();
			cursor = getCursorFromList(rs);
		}

		else{
			ArrayList<String> hashed_ports = new ArrayList<String>(5);
			int coordinator = -1;
			String hash_key = "";
			try {
				hash_key = genHash(selection);
				for(String i : REMOTE_PORTS.split(",")){
					hashed_ports.add(genHash(i.trim()));
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			if(hash_key.compareTo(hashed_ports.get(0))<= 0){ coordinator = 0;}
			else if(hash_key.compareTo(hashed_ports.get(1))<= 0){ coordinator = 1;}
			else if(hash_key.compareTo(hashed_ports.get(2))<= 0){ coordinator = 2;}
			else if(hash_key.compareTo(hashed_ports.get(3))<= 0){ coordinator = 3;}
			else if(hash_key.compareTo(hashed_ports.get(4))<= 0){ coordinator = 4;}
			else{coordinator = 0;}

			for(int i=0; i<3; i++) {		//not essential
				int ind = (coordinator + i) % 5;
				String port = REMOTE_PORTS.split(",")[ind].trim();

				try {
					Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port) * 2);

					DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());
					Thread.sleep(10);
					succ_out.writeUTF(QUERY_SINGLE + ":" + selection + " ");
					succ_out.flush();
					Log.d(TAG, "Request sent > " + QUERY_SINGLE + ":" + selection + " > " + port);

					DataInputStream succ_in = new DataInputStream(succ_socket.getInputStream());
					String inp_msg = succ_in.readUTF();
					if (inp_msg != null) {
						Log.d(TAG, "Message recieved > " + inp_msg);
						Log.d(TAG, "Length of recieced message : " + inp_msg.split(",").length);
						if (inp_msg.split(",").length != 2){continue;}
						else {
							cursor = getCursorFromList(inp_msg.trim());
							Log.d(TAG, "Cursor size > " + cursor.getCount());
							break;
						}

					}
					succ_out.close();
					succ_in.close();
					succ_socket.close();
				} catch (IOException e) {
					e.printStackTrace();
					Log.e("Query * call", "Exception");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return cursor;
	}

	public Cursor query_helper(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		SQLiteDatabase db = dbh.getWritableDatabase();
		Cursor cursor = db.query(
				DBHelper.TABLE_NAME,   // The table to query
				null,             // The array of columns to return (pass null to get all)
				"key='" + selection + "'",              // The columns for the WHERE clause
				selectionArgs,          // The values for the WHERE clause
				null,                   // don't group the rows
				null,                   // don't filter by row groups
				null               // The sort order
		);
		return cursor;
	}
		@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		SQLiteDatabase db = dbh.getWritableDatabase();
		int row_count = 0;
		if(selection.equals("@")){
			//https://stackoverflow.com/questions/9599741/how-to-delete-all-record-from-table-in-sqlite-with-android
			row_count = db.delete(TABLE_NAME, "1", null);
		}
		else if(selection.equals("*")){
			for (String port : REMOTE_PORTS.split(",")) {
				try {
					port = port.trim();
					Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port) * 2);

					DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());
					Thread.sleep(10);
					succ_out.writeUTF(DELETE + ":" + "*" + " ");
					succ_out.flush();

				} catch (IOException e) {
					e.printStackTrace();
					Log.e("Delete * call", "Exception");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		else{
			String TAG = DELETE_SINGLE;
			ArrayList<String> hashed_ports = new ArrayList<String>(5);
			int coordinator = -1;

			String hash_key = "";
			try {
				hash_key = genHash(selection);
				for(String i : REMOTE_PORTS.split(",")){
					hashed_ports.add(genHash(i.trim()));
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			if(hash_key.compareTo(hashed_ports.get(0))< 0){ coordinator = 0;}
			else if(hash_key.compareTo(hashed_ports.get(1))<= 0){ coordinator = 1;}
			else if(hash_key.compareTo(hashed_ports.get(2))<= 0){ coordinator = 2;}
			else if(hash_key.compareTo(hashed_ports.get(3))<= 0){ coordinator = 3;}
			else if(hash_key.compareTo(hashed_ports.get(4))<= 0){ coordinator = 4;}
			else{coordinator = 0;}

			//get list of ports to which the data is to be deleted
			String[] Remote_Port_List = REMOTE_PORTS.split(",");
			for(int i=0; i<3; i++){
				int ind = (coordinator + i)%5;
				String port = Remote_Port_List[ind].trim();

				try {
					Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port)*2);
					DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());

					Thread.sleep(10);

					succ_out.writeUTF(DELETE_SINGLE + ":" + selection );
					succ_out.flush();

					Log.d(TAG, selection + " sent to > " + port);
					succ_out.close();
					succ_socket.close();

				} catch (IOException e) {
					e.printStackTrace();
					Log.e(TAG,"Exception");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		//Log.d("Delete", "Delete count > " + row_count);
		return row_count;
	}

	public int delete_single(Uri uri, String selection, String[] selectionArgs) {
		SQLiteDatabase db = dbh.getWritableDatabase();
		int row_count = db.delete(TABLE_NAME, key + " = '" + selection + "'", null);
		Log.d(TAG,"Delete count in AVD " + myPort + " > " + row_count);
		Log.d("Delete", "Delete count > " + row_count);
		return row_count;
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
					String msgRecieved = in.readUTF().trim();

					if (null != msgRecieved) {
						Log.d("SERVER_"+myPort,"Socket Accepted");
						String[] msgs = msgRecieved.split(":");
						Log.d("SERVER_REC" , msgRecieved);
						String TAG_input = msgs[0].trim();

						String payload = msgs[1].trim();
						Log.d(TAG,"Executing case: " + TAG_input + " Payload Value " + payload);

						//NEW NODE ENTRY at 5554
						if (TAG_input.equals(NEW_NODE_ENTRY)) {

							String TAG = NEW_NODE_ENTRY;
							Log.d(TAG, "Payload received : " + payload);

							Log.d(TAG,"Ports list before : " + REMOTE_PORTS);
							Log.d("ENTER RECOVERY:", String.valueOf(REMOTE_PORTS.contains(payload)));

							Boolean check = payload.equals("5554") && check();
							Log.d("5554 RECOVERY:", check.toString());

							if(check){
								//check if 5556 exists
								REMOTE_PORTS = "5562, 5556, 5554, 5558, 5560";
							}
                            Log.d("ENTER RECOVERY:", String.valueOf(REMOTE_PORTS.contains(payload)));

							if(REMOTE_PORTS.contains(payload)) {
								REMOTE_PORTS = "5562, 5556, 5554, 5558, 5560";
								Log.d("CHECK:",payload + " in " + REMOTE_PORTS);

								Socket new_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(payload) * 2);
                                DataOutputStream succ_out = new DataOutputStream(new_socket.getOutputStream());

                                Thread.sleep(10);
                                succ_out.writeUTF(RECOVERY + ":" + "dummy");
                                succ_out.flush();
                                Log.d(TAG, RECOVERY + ":" + "dummy");
                                succ_out.close();
                                new_socket.close();
							}
							else {
								//add the node to REMOTE PORTS list;
								if (!REMOTE_PORTS.equals("")) {
									REMOTE_PORTS += ",";
								} //add "," if not the first port in list
								REMOTE_PORTS += payload;

								//Log.d(TAG,REMOTE_PORTS);
								Log.d(TAG, "Ports list after : " + REMOTE_PORTS);

								//sort the REMOTE_PORT list
								REMOTE_PORTS = sort_ports(REMOTE_PORTS);
								Log.d(TAG, "Ports list sorted : " + REMOTE_PORTS);


								Log.d(TAG, "Predecessor, Successor : " + Predecessor + "," + Successor);
								//update predecessor
								//update successor
								String[] ports_ar = REMOTE_PORTS.split(",");
								String[] ports_arr = new String[ports_ar.length];
								for (int i = 0; i < ports_ar.length; i++) {
									ports_arr[i] = ports_ar[i].trim();
								}
								if (ports_arr.length > 1) {
									int my_ind = Arrays.asList(ports_arr).indexOf(myPort);
									int len = ports_arr.length;
									int pred_ind = (my_ind - 1) % len;
									pred_ind = pred_ind >= 0 ? pred_ind : len + pred_ind;
									int succ_ind = (my_ind + 1) % len;
									Predecessor = ports_arr[pred_ind];
									Successor = ports_arr[succ_ind];
								}
								Log.d(TAG, "Predecessor, Successor : " + Predecessor + "," + Successor + " myport: " + myPort);

								String first_port = "5554";

								//send the new node to successor
								if (!Successor.isEmpty()) { // && !Successor.equals(first_port)) {
									Log.d(TAG, "Sending to Successor");
									Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor) * 2);
									DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());
									Thread.sleep(10);
									succ_out.writeUTF(NEW_NODE + ":" + REMOTE_PORTS + " ");
									succ_out.flush();
									Log.d(TAG, NEW_NODE + ":" + REMOTE_PORTS);
									succ_out.close();
									succ_socket.close();
								}
							}
						}

						else if(TAG_input.equals(RECOVERY)){
							REMOTE_PORTS = "5562, 5556, 5554, 5558, 5560";
							String TAG = RECOVERY;
                            //REMOTE_PORTS = payload;
                            Log.d(TAG, "Updated REMOTE_PORTS > " + REMOTE_PORTS);

                            String pred1 = "";
                            String pred2 = "";
                            String succ1 = "";
                            String succ2 = "";
                            String[] ports_lst = REMOTE_PORTS.split(",");
                            for (int i = 0; i < ports_lst.length; i++) {
                                if (ports_lst[i].trim().compareTo(myPort.trim()) == 0) {
                                    if (i == 0) {
                                        pred1 = ports_lst[3].trim();
                                        pred2 = ports_lst[4].trim();
                                        succ1 = ports_lst[1].trim();
                                        succ2 = ports_lst[2].trim();
                                    } else if (i == 1) {
                                        pred1 = ports_lst[4].trim();
                                        pred2 = ports_lst[0].trim();
                                        succ1 = ports_lst[2].trim();
                                        succ2 = ports_lst[3].trim();
                                    } else if (i == 2) {
                                        pred1 = ports_lst[0].trim();
                                        pred2 = ports_lst[1].trim();
                                        succ1 = ports_lst[3].trim();
                                        succ2 = ports_lst[4].trim();
                                    } else if (i == 3) {
                                        pred1 = ports_lst[1].trim();
                                        pred2 = ports_lst[2].trim();
                                        succ1 = ports_lst[4].trim();
                                        succ2 = ports_lst[0].trim();
                                    } else if (i ==4){
                                        pred1 = ports_lst[2].trim();
                                        pred2 = ports_lst[3].trim();
                                        succ1 = ports_lst[0].trim();
                                        succ2 = ports_lst[1].trim();
                                    }
                                }
                            }

                            //get predecessor1 local data
                            //get predecessor2 local data
                            ArrayList<String> results = new ArrayList<String>();

                            ArrayList<String> preds = new ArrayList<String>(2);
                            preds.add(pred1);
                            preds.add(pred2);
                            for (String p : preds) {
                                try {
                                    Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p) * 2);

                                    DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());
                                    Thread.sleep(10);
                                    succ_out.writeUTF(GET_LOCAL + ":" + "dummy" + " ");
                                    succ_out.flush();
                                    Log.d(TAG, "Request sent > " + GET_LOCAL + ":" + "dummy");

                                    DataInputStream succ_in = new DataInputStream(succ_socket.getInputStream());
                                    String inp_msg = succ_in.readUTF().trim();
                                    if (null != inp_msg) {
                                        Log.d(TAG, "Message recieved from self > " + inp_msg);
                                        int len = inp_msg.split(",").length;
                                        Log.d(TAG, "Length of recieced message : " + len);
                                        if(len%2 == 0){
	                                        results.add(inp_msg);}
                                    }
                                    succ_socket.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    Log.e(GET_LOCAL, "Exception");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            //get replicas of myPort in successor1
                            //if successor1 failed, get replicas of myPort in successor2

                            ArrayList<String> succs = new ArrayList<String>(2);
                            succs.add(succ1);
                            succs.add(succ2);
                            for (String s : succs) {
                                try {
                                    Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s) * 2);

                                    DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());
                                    Thread.sleep(10);

                                    succ_out.writeUTF(GET_REPLICA + ":" + myPort + " ");
                                    succ_out.flush();

                                    Log.d(TAG, "Request sent > " + GET_REPLICA + ":" + myPort);

                                    DataInputStream succ_in = new DataInputStream(succ_socket.getInputStream());
                                    String inp_msg = succ_in.readUTF().trim();
                                    if (null != inp_msg) {
                                        Log.d(TAG, "Message recieved from self > " + inp_msg);
										int len = inp_msg.split(",").length;
										Log.d(TAG, "Length of recieced message : " + len);
										if(len%2 == 0){
											results.add(inp_msg);}
                                    }
                                    succ_socket.close();
//                                    break;
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    Log.e(GET_LOCAL, "Exception");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            //all gets as string
                            //concat all the strings
                            String result = "";
                            for (String i : results) {
                                result += i;
                                result += ",";
                            }
                            int i = 0;
                            int j = result.length() - 1;
                            while (result.charAt(i) == ',') {
                                i++;
                            }
                            while (result.charAt(j) == ',') {
                                j--;
                            }
                            result = result.substring(i, j + 1);

                            Log.d(TAG, "Final result string: " + result);

                            //insert <key,value> pair one by one
                            String[] vals = result.split(",");
                            for (i = 0; i < vals.length; i += 2) {
                                ContentValues cv = new ContentValues();

                                String KEY_I = vals[i].trim();
                                String VALUE_I = vals[i + 1].trim();

                                cv.put(key, KEY_I);
                                cv.put(value, VALUE_I);

                                Uri t_uri = insert(mUri, cv, "I");
                                Log.d(TAG,"Inserted " + KEY_I);
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
								//String old_Predecessor = Predecessor;
								//String old_Successor = Successor;
								Predecessor = ports_arr[pred_ind];
								Successor = ports_arr[succ_ind];

								Log.d(TAG, "Updated Predecessor, Successor > " + Predecessor + "," + Successor+" myport: "+myPort);
							}


							String first_port = "5554";

							if (!Successor.isEmpty() && !Successor.equals(first_port)) {

								Socket succ_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(Successor)*2);
								DataOutputStream succ_out = new DataOutputStream(succ_socket.getOutputStream());
								Thread.sleep(10);
								succ_out.writeUTF(NEW_NODE + ":" + REMOTE_PORTS +" ");
								succ_out.flush();
								succ_out.close();
								succ_socket.close();
							}
						}

						else if(TAG_input.equals(DELETE)){
							delete(mUri,"@",null);
						}

						else if(TAG_input.equals(QUERY)){
							Cursor cursor_local =  query(mUri, null, "@",null,null);
							String msg_load = cursor_to_string(cursor_local);
							msg_load = msg_load.trim();
							Log.d(TAG,"Mesasge from local AVD : " + msg_load);
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
							Uri t_uri = insert(mUri,cv,"R");

							String origin_port = key_val[2].trim();
							if(map.get(origin_port) != null){
							map.put(origin_port, map.get(origin_port).trim() + "," + KEY_I + "," + VALUE_I);}
							else{ map.put(origin_port,  KEY_I + "," + VALUE_I);}
						}

						else if(TAG_input.equals(QUERY_SINGLE)){
							String TAG = QUERY_SINGLE;
							String return_msg = "";
							String KEY_I = payload.trim();
							Cursor cursor = query_helper(mUri,null,KEY_I,null, null);
							return_msg = cursor_to_string(cursor);

							Log.d(TAG, "Message to send > " + return_msg + " to > "  + "requester");
							DataOutputStream pred_out = new DataOutputStream(socket.getOutputStream());
							Thread.sleep(10);
							pred_out.writeUTF(return_msg  + " ");
							pred_out.flush();
						}

						else if(TAG_input.equals(DELETE_SINGLE)){
							String KEY_I = payload.trim();
							delete_single(mUri,KEY_I,null);
						}

						else if(TAG_input.equals(GET_LOCAL)){
							DataOutputStream out = new DataOutputStream(socket.getOutputStream());
							String return_msg = map.get(myPort);
							Thread.sleep(10);
							out.writeUTF(return_msg + " ");
							out.flush();
							Log.d(TAG, "Message to send > " + return_msg + " to > "  + "requester");
						}

						else if(TAG_input.equals(GET_REPLICA)){
							DataOutputStream out = new DataOutputStream(socket.getOutputStream());
							String return_msg = map.get(payload);
							Thread.sleep(10);
							out.writeUTF(return_msg + " ");
							out.flush();
							Log.d(TAG, "Message to send > " + return_msg + " to > "  + "requester");
						}

						else if(TAG_input.equals("dummy")){
							DataOutputStream d_out = new DataOutputStream(socket.getOutputStream());
							Thread.sleep(10);
							d_out.writeUTF("ACK");
							d_out.flush();
							d_out.close();
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

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public boolean check(){
		try {
			Socket new_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("5556") * 2);
			DataOutputStream succ_out = new DataOutputStream(new_socket.getOutputStream());

			Thread.sleep(10);
			succ_out.writeUTF("dummy:0");
			succ_out.flush();
			Log.d(TAG, "dummy:0");

			DataInputStream succ_in = new DataInputStream(new_socket.getInputStream());
			String inp = succ_in.readUTF().trim();
			if(inp!=null){
				if(inp.equals("ACK")){
					succ_out.close();
					new_socket.close();

					return true;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}
}

//5562, 5556, 5554, 5558, 5560