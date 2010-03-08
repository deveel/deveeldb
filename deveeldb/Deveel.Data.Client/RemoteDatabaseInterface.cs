// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;

using Deveel.Data.Util;

namespace Deveel.Data.Client {
	/// <summary>
	/// An abstract implementation of <see cref="IDatabaseInterface"/> that retrieves 
	/// information from a remote server host.
	/// </summary>
	/// <remarks>
	/// The actual implementation of the communication protocol is left to the derived 
	/// classes.
	/// </remarks>
	abstract class RemoteDatabaseInterface : IDatabaseInterface {
		/// <summary>
		/// The thread that dispatches commands to the server.
		/// </summary>
		/// <remarks>
		/// This is created and started after the <see cref="Login"/> method is 
		/// called. This can handle concurrent queries through the protocol pipe.
		/// </remarks>
		private ConnectionThread connection_thread;

		/// <summary>
		/// A <see cref="IDatabaseCallBack"/> implementation that is notified of 
		/// all events that are received from the database.
		/// </summary>
		private IDatabaseCallBack database_call_back;

		/// <summary>
		/// The initial database for the connection.
		/// </summary>
		private readonly string initial_database;

		/// <summary>
		/// The version of the server this interface is connected to.
		/// </summary>
		private Version serverVersion = new Version();

		/// <summary>
		/// Constructs a new <see cref="RemoteDatabaseInterface"/> which points to the
		/// given initial database.
		/// </summary>
		/// <param name="database">The initial database for the connection.</param>
		internal RemoteDatabaseInterface(string database) {
			initial_database = database;
		}

		/// <summary>
		/// Gets the version of the server this interface is connected to.
		/// </summary>
		public Version ServerVersion {
			get { return serverVersion; }
		}

		public Version ClientVersion {
			get { return Assembly.GetCallingAssembly().GetName().Version; }
		}

		/// <summary>
		/// Writes the exception to the log stream.
		/// </summary>
		/// <param name="e"></param>
		private static void logException(Exception e) {
			//TODO:
		}


		// ---------- Abstract methods ----------

		/// <summary>
		/// Writes the given command to the server.
		/// </summary>
		/// <param name="command"></param>
		/// <param name="offset"></param>
		/// <param name="length"></param>
		/// <remarks>
		/// The way the command is written is totally network layer dependent.
		/// </remarks>
		internal abstract void WriteCommandToServer(byte[] command, int offset, int length);

		/// <summary>
		/// Blocks until the next command is received from the server.
		/// </summary>
		/// <param name="timeout"></param>
		/// <remarks>
		/// The way this is implemented is network layer dependant.
		/// </remarks>
		/// <returns></returns>
		internal abstract byte[] NextCommandFromServer(int timeout);

		/// <summary>
		/// Closes the connection.
		/// </summary>
		internal abstract void CloseConnection();


		/// <inheritdoc/>
		public bool Login(string default_schema, string user, string password, IDatabaseCallBack call_back) {
			try {

				// Do some handshaking,
				MemoryStream bout = new MemoryStream();
				BinaryWriter output = new BinaryWriter(bout, Encoding.ASCII);

				// Write output the magic number
				output.Write(0x0ced007);
				// Write output the driver version
				Version clientVersion = ClientVersion;
				output.Write(clientVersion.Major);
				output.Write(clientVersion.Minor);
				output.Write(initial_database);
				byte[] arr = bout.ToArray();
				WriteCommandToServer(arr, 0, arr.Length);

				byte[] response = NextCommandFromServer(0);

				//      printByteArray(response);

				int ack = ByteBuffer.ReadInt4(response, 0);
				if (ack == ProtocolConstants.ACKNOWLEDGEMENT) {
					// Is there anything more to Read?
					if (response.Length > 4 && response[4] == 1) {
						// Yes so Read the server version
						int serverMajorVersion = ByteBuffer.ReadInt4(response, 5);
						int serverMinorVersion = ByteBuffer.ReadInt4(response, 9);
						serverVersion = new Version(serverMajorVersion, serverMinorVersion);
					}

					// Send the username and password to the server
					// SECURITY: username/password sent as plain text.  This is okay
					//   if we are connecting to localhost, but not good if we connecting
					//   over the internet.  We could encrypt this, but it would probably
					//   be better if we WriteByte the entire stream through an encyption
					//   protocol.

					bout = new MemoryStream();
					output = new BinaryWriter(bout);
					output.Write(default_schema);
					output.Write(user);
					output.Write(password);
					arr = bout.ToArray();
					WriteCommandToServer(arr, 0, arr.Length);

					response = NextCommandFromServer(0);
					int result = ByteBuffer.ReadInt4(response, 0);
					if (result == ProtocolConstants.USER_AUTHENTICATION_PASSED) {

						// Set the call_back,
						this.database_call_back = call_back;

						// User authentication passed so we successfully logged input now.
						connection_thread = new ConnectionThread(this);
						connection_thread.Start();
						return true;

					} else if (result == ProtocolConstants.USER_AUTHENTICATION_FAILED) {
						throw new DataException("User Authentication failed.");
					} else if (result == ProtocolConstants.DATABASE_NOT_FOUND) {
						throw new DataException("The database specified was not found.");
					} else {
						throw new DataException("Unexpected response.");
					}
				} else if (ack == ProtocolConstants.DATABASE_NOT_FOUND) {
					throw new DataException("The database was not found.");
				} else {
					throw new DataException("No acknowledgement received from server.");
				}

			} catch (IOException e) {
				logException(e);
				throw new DataException("IOException: " + e.Message);
			}

		}

		/// <inheritdoc/>
		public void PushStreamableObjectPart(ReferenceType type, long object_id, long object_length, byte[] buf, long offset, int length) {
			try {
				// Push the object part
				int dispatch_id = connection_thread.PushStreamableObjectPart(type, object_id, object_length, buf, offset, length);
				// Get the response
				ServerCommand command = connection_thread.GetCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatch_id);
				// If command == null then we timed output
				if (command == null) {
					throw new DataException("Query timed output after " +
										   DeveelDbConnection.QUERY_TIMEOUT + " seconds.");
				}

				BinaryReader din = new BinaryReader(command.GetInputStream());
				int status = din.ReadInt32();

				// If failed report the error.
				if (status == ProtocolConstants.FAILED) {
					throw new DataException("Push object failed: " + din.ReadString());
				}

			} catch (IOException e) {
				logException(e);
				throw new DataException("IO Error: " + e.Message);
			}

		}

		public void ChangeDatabase(string database) {
			try {
				// Change the current database
				int dispatch_id = connection_thread.ChangeDatabase(database);
				// get the response
				ServerCommand command = connection_thread.GetCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatch_id);
				if (command == null)
					throw new DataException("Query timed output after " + DeveelDbConnection.QUERY_TIMEOUT + " seconds.");

				BinaryReader din = new BinaryReader(command.GetInputStream());
				int status = din.ReadInt32();
				if (status == ProtocolConstants.FAILED)
					throw new DataException("Change database failed: " + din.ReadString());
				if (status == ProtocolConstants.DATABASE_NOT_FOUND)
					throw new DataException("The database '" + database + "' was not found on the server.");
			} catch(IOException e) {
				logException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public IQueryResponse ExecuteQuery(SqlQuery sql) {
			try {
				// Execute the command
				int dispatch_id = connection_thread.ExecuteQuery(sql);
				// Get the response
				ServerCommand command =
						connection_thread.GetCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatch_id);
				// If command == null then we timed output
				if (command == null) {
					throw new DataException("Query timed output after " +
										   DeveelDbConnection.QUERY_TIMEOUT + " seconds.");
				}

				BinaryReader input = new BinaryReader(command.GetInputStream());

				// Query response protocol...
				int status = input.ReadInt32();
				if (status == ProtocolConstants.SUCCESS) {
					int result_id = input.ReadInt32();
					int query_time = input.ReadInt32();
					int row_count = input.ReadInt32();
					int col_count = input.ReadInt32();
					ColumnDescription[] col_list = new ColumnDescription[col_count];
					for (int i = 0; i < col_count; ++i) {
						col_list[i] = ColumnDescription.ReadFrom(input);
					}

					return new QueryResponseImpl(result_id, query_time, col_count, row_count, col_list);

				} else if (status == ProtocolConstants.EXCEPTION) {
					int db_code = input.ReadInt32();
					String message = input.ReadString();
					String stack_trace = input.ReadString();
					//        Console.Out.WriteLine("**** DUMP OF SERVER STACK TRACE OF Error:");
					//        Console.Out.WriteLine(stack_trace);
					//        Console.Out.WriteLine("**** ----------");
					throw new DbDataException(message, null, db_code, stack_trace);
				} else if (status == ProtocolConstants.AUTHENTICATION_ERROR) {
					// Means we could perform the command because user doesn't have enough
					// rights.
					String access_type = input.ReadString();
					String table_name = input.ReadString();
					throw new DataException("User doesn't have enough privs to " +
										   access_type + " table " + table_name);
				} else {
					throw new DataException("Illegal response code from server.");
				}

			} catch (IOException e) {
				logException(e);
				throw new DataException("IO Error: " + e.Message);
			}

		}

		private class QueryResponseImpl : IQueryResponse {
			private readonly int result_id;
			private readonly int query_time;
			private readonly int col_count;
			private readonly int row_count;
			private readonly ColumnDescription[] col_list;

			public QueryResponseImpl(int resultId, int queryTime, int colCount, int rowCount, ColumnDescription[] colList) {
				result_id = resultId;
				col_list = colList;
				row_count = rowCount;
				col_count = colCount;
				query_time = queryTime;
			}

		    public int ResultId {
		        get { return result_id; }
		    }

		    public int QueryTimeMillis {
		        get { return query_time; }
		    }

		    public int RowCount {
		        get { return row_count; }
		    }

		    public int ColumnCount {
		        get { return col_count; }
		    }

		    public ColumnDescription GetColumnDescription(int n) {
				return col_list[n];
			}

		    public string Warnings {
		        get { return ""; }
		    }
		}

		/// <inheritdoc/>
		public ResultPart GetResultPart(int result_id, int start_row, int count_rows) {

			try {

				// Get the first few rows of the result..
				int dispatch_id = connection_thread.GetResultPart(result_id,
																  start_row, count_rows);

				// Get the response
				ServerCommand command =
						connection_thread.GetCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatch_id);
				// If command == null then we timed output
				if (command == null) {
					throw new DataException("Downloading result part timed output after " +
										   DeveelDbConnection.QUERY_TIMEOUT + " seconds.");
				}

				// Wrap around a DataInputStream
				BinaryReader din = new BinaryReader(command.GetInputStream());
				int status = din.ReadInt32();

				if (status == ProtocolConstants.SUCCESS) {
					// Return the contents of the response.
					int col_count = din.ReadInt32();
					int size = count_rows * col_count;
					ResultPart list = new ResultPart(size);
					for (int i = 0; i < size; ++i) {
						list.Add(ObjectTransfer.ReadFrom(din));
					}
					return list;
				} else if (status == ProtocolConstants.EXCEPTION) {
					int db_code = din.ReadInt32();
					String message = din.ReadString();
					String stack_trace = din.ReadString();
					//        Console.Out.WriteLine("**** DUMP OF SERVER STACK TRACE OF Error:");
					//        Console.Out.WriteLine(stack_trace);
					//        Console.Out.WriteLine("**** ----------");
					throw new DbDataException(message, message, db_code, stack_trace);
				} else {
					throw new DataException("Illegal response code from server.");
				}

			} catch (IOException e) {
				logException(e);
				throw new DataException("IO Error: " + e.Message);
			}

		}

		/// <inheritdoc/>
		public void DisposeResult(int result_id) {
			try {
				int dispatch_id = connection_thread.DisposeResult(result_id);
				// Get the response
				ServerCommand command =
						connection_thread.GetCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatch_id);
				// If command == null then we timed output
				if (command == null) {
					throw new DataException("Dispose result timed output after " +
										   DeveelDbConnection.QUERY_TIMEOUT + " seconds.");
				}

				// Check the dispose was successful.
				BinaryReader din = new BinaryReader(command.GetInputStream());
				int status = din.ReadInt32();

				// If failed report the error.
				if (status == ProtocolConstants.FAILED) {
					throw new DataException("Dispose failed: " + din.ReadString());
				}

			} catch (IOException e) {
				logException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public StreamableObjectPart GetStreamableObjectPart(int result_id,
			  long streamable_object_id, long offset, int len) {
			try {
				int dispatch_id = connection_thread.GetStreamableObjectPart(result_id,
													 streamable_object_id, offset, len);
				ServerCommand command =
						connection_thread.GetCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatch_id);
				// If command == null then we timed output
				if (command == null) {
					throw new DataException("GetStreamableObjectPart timed output after " +
										   DeveelDbConnection.QUERY_TIMEOUT + " seconds.");
				}

				BinaryReader din = new BinaryReader(command.GetInputStream());
				int status = din.ReadInt32();

				if (status == ProtocolConstants.SUCCESS) {
					// Return the contents of the response.
					int contents_size = din.ReadInt32();
					byte[] buf = new byte[contents_size];
					din.Read(buf, 0, contents_size);
					return new StreamableObjectPart(buf);
				} else if (status == ProtocolConstants.EXCEPTION) {
					int db_code = din.ReadInt32();
					String message = din.ReadString();
					String stack_trace = din.ReadString();
					throw new DbDataException(message, message, db_code, stack_trace);
				} else {
					throw new DataException("Illegal response code from server.");
				}

			} catch (IOException e) {
				logException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public void DisposeStreamableObject(int result_id, long streamable_object_id) {
			try {
				int dispatch_id = connection_thread.DisposeStreamableObject(
														result_id, streamable_object_id);
				ServerCommand command =
						connection_thread.GetCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatch_id);
				// If command == null then we timed output
				if (command == null) {
					throw new DataException("DisposeStreamableObject timed output after " +
										   DeveelDbConnection.QUERY_TIMEOUT + " seconds.");
				}

				BinaryReader din = new BinaryReader(command.GetInputStream());
				int status = din.ReadInt32();

				// If failed report the error.
				if (status == ProtocolConstants.FAILED) {
					throw new DataException("Dispose failed: " + din.ReadString());
				}

			} catch (IOException e) {
				logException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public void Dispose() {
			try {
				int dispatch_id = connection_thread.SendCloseCommand();
				//      // Get the response
				//      ServerCommand command =
				//            connection_thread.GetCommand(MDriver.QUERY_TIMEOUT, dispatch_id);
				CloseConnection();
			} catch (IOException e) {
				logException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// The connection thread that can dispatch commands concurrently through 
		/// the input/output pipe.
		/// </summary>
		private class ConnectionThread {
			/// <summary>
			/// The actual running thread.
			/// </summary>
			private readonly Thread thread;
			/// <summary>
			/// 
			/// </summary>
			private RemoteDatabaseInterface remote_interface;
			/// <summary>
			/// The command to write output to the server.
			/// </summary>
			private MemoryStream com_bytes;
			private BinaryWriter com_data;

			/// <summary>
			/// Running dispatch id values which we use as a unique key.
			/// </summary>
			private int running_dispatch_id = 1;

			/// <summary>
			/// Set to true when the thread is closed.
			/// </summary>
			private readonly bool thread_closed;

			/// <summary>
			/// The list of commands received from the server that are pending to be
			/// processed (ServerCommand).
			/// </summary>
			private ArrayList commands_list;

			/// <summary>
			/// Constructs the connection thread.
			/// </summary>
			/// <param name="remote_interface"></param>
			internal ConnectionThread(RemoteDatabaseInterface remote_interface) {
				this.remote_interface = remote_interface;
				thread = new Thread(new ThreadStart(run));
				thread.IsBackground = true;
				thread.Name = "Connection Thread";
				com_bytes = new MemoryStream();
				com_data = new BinaryWriter(com_bytes, Encoding.Unicode);

				commands_list = new ArrayList();
				thread_closed = false;
			}

			// ---------- Utility ----------

			/// <summary>
			/// Returns a unique dispatch id number for a command.
			/// </summary>
			/// <returns></returns>
			private int NextDispatchId() {
				return running_dispatch_id++;
			}

			/**
			 * Blocks until a response from the server has been received with the
			 * given dispatch id.  It waits for 'timeout' seconds and if the response
			 * hasn't been received by then returns null.
			 */

			internal ServerCommand GetCommand(int timeout, int dispatch_id) {
				DateTime time_in = DateTime.Now;
				DateTime time_out_high = time_in + new TimeSpan(((long)timeout * 1000) * TimeSpan.TicksPerMillisecond);

				lock (commands_list) {

					if (commands_list == null) {
						throw new DataException("Connection to server closed");
					}

					while (true) {

						for (int i = 0; i < commands_list.Count; ++i) {
							ServerCommand command = (ServerCommand)commands_list[i];
							if (command.DispatchId == dispatch_id) {
								commands_list.RemoveAt(i);
								return command;
							}
						}

						// Return null if we haven't received a response input the timeout
						// period.
						if (timeout != 0 &&
							DateTime.Now > time_out_high) {
							return null;
						}

						// Wait a second.
						try {
							Monitor.Wait(commands_list, 1000);
						} catch (ThreadInterruptedException) { /* ignore */ }

					} // while (true)

				} // lock

			}


			// ---------- Server request methods ----------

			/// <summary>
			/// Flushes the command input 'com_bytes' to the server.
			/// </summary>
			private void FlushCommand() {
				lock (this) {
					// We flush the size of the command string followed by the command
					// itself to the server.  This format allows us to implement a simple
					// non-blocking command parser on the server.
					remote_interface.WriteCommandToServer(com_bytes.GetBuffer(), 0, (int)com_bytes.Length);
					com_bytes = new MemoryStream();
					com_data = new BinaryWriter(com_bytes, Encoding.Unicode);
				}
			}

			internal int ChangeDatabase(string database) {
				lock (this) {
					int dispatch_id = NextDispatchId();
					com_data.Write(ProtocolConstants.CHANGE_DATABASE);
					com_data.Write(dispatch_id);
					com_data.Write(database);
					FlushCommand();

					return dispatch_id;
				}
			}

			/// <summary>
			/// Pushes a part of a streamable object onto the server.
			/// </summary>
			/// <param name="type"></param>
			/// <param name="object_id"></param>
			/// <param name="object_length"></param>
			/// <param name="buf"></param>
			/// <param name="offset"></param>
			/// <param name="length"></param>
			/// <remarks>
			/// Used input preparation to executing queries containing large objects.
			/// </remarks>
			/// <returns></returns>
			internal int PushStreamableObjectPart(ReferenceType type, long object_id,
					   long object_length, byte[] buf, long offset, int length) {
				lock (this) {
					int dispatch_id = NextDispatchId();
					com_data.Write(ProtocolConstants.PUSH_STREAMABLE_OBJECT_PART);
					com_data.Write(dispatch_id);
					com_data.Write((byte)type);
					com_data.Write(object_id);
					com_data.Write(object_length);
					com_data.Write(length);
					com_data.Write(buf, 0, length);
					com_data.Write(offset);
					FlushCommand();

					return dispatch_id;
				}
			}

			/// <summary>
			/// Sends a command to the server to process a command.
			/// </summary>
			/// <param name="sql"></param>
			/// <remarks>
			/// The response from the server will contain a 'result_id' that is a unique 
			/// number for refering to the result. It also contains information about the 
			/// columns input the table, and the total number of rows input the result.
			/// </remarks>
			/// <returns>
			/// Returns the dispatch id key for the response from the server.
			/// </returns>
			internal int ExecuteQuery(SqlQuery sql) {
				lock (this) {
					int dispatch_id = NextDispatchId();
					com_data.Write(ProtocolConstants.QUERY);
					com_data.Write(dispatch_id);
					sql.WriteTo(com_data);
					FlushCommand();

					return dispatch_id;
				}
			}

			/// <summary>
			/// Releases the server side resources associated with a given command 
			/// key returned by the server.
			/// </summary>
			/// <param name="result_id"></param>
			/// <remarks>
			/// This should be called when the <see cref="ResultSet"/> is closed, 
			/// or if we cancel input the middle of downloading a result.
			/// <para>
			/// It's very important that the server resources for a command is released.
			/// </para>
			/// </remarks>
			/// <returns>
			/// Returns the dispatch id key for the response from the server.
			/// </returns>
			internal int DisposeResult(int result_id) {
				lock (this) {
					int dispatch_id = NextDispatchId();
					com_data.Write(ProtocolConstants.DISPOSE_RESULT);
					com_data.Write(dispatch_id);
					com_data.Write(result_id);
					FlushCommand();

					return dispatch_id;
				}
			}

			/// <summary>
			/// Requests a part of a result of a command.
			/// </summary>
			/// <param name="result_id">The identifier of the result to download from: this
			/// is generated by the <c>command</c> command.</param>
			/// <param name="row_number">The row to download from.</param>
			/// <param name="row_count">The number of rows to download.</param>
			/// <remarks>
			/// This is used to download a part of a result set from the server. 
			/// This will generate an error if the <paramref name="result_id"/> is 
			/// invalid or has previously been disposed.
			/// </remarks>
			/// <returns>
			/// Returns the dispatch id key for the response from the server.
			/// </returns>
			internal int GetResultPart(int result_id, int row_number, int row_count) {
				lock (this) {
					int dispatch_id = NextDispatchId();
					com_data.Write(ProtocolConstants.RESULT_SECTION);
					com_data.Write(dispatch_id);
					com_data.Write(result_id);
					com_data.Write(row_number);
					com_data.Write(row_count);
					FlushCommand();

					return dispatch_id;
				}
			}

			/// <summary>
			/// Requests a part of an open <see cref="StreamableObject"/> channel.
			/// </summary>
			/// <param name="result_id"></param>
			/// <param name="streamable_object_id">The identifier of the object to
			/// download: this is returned by <see cref="Data.StreamableObject.Identifier"/></param>
			/// <param name="offset"></param>
			/// <param name="length"></param>
			/// <remarks>
			/// This is used to download a section of a large object, such as a 
			/// <see cref="IBlob"/> or a <see cref="IClob"/>.
			/// </remarks>
			/// <returns>
			/// Returns the dispatch id key for the response from the server.
			/// </returns>
			internal int GetStreamableObjectPart(int result_id, long streamable_object_id,
										 long offset, int length) {
				lock (this) {
					int dispatch_id = NextDispatchId();
					com_data.Write(ProtocolConstants.STREAMABLE_OBJECT_SECTION);
					com_data.Write(dispatch_id);
					com_data.Write(result_id);
					com_data.Write(streamable_object_id);
					com_data.Write(offset);
					com_data.Write(length);
					FlushCommand();

					return dispatch_id;
				}
			}

			/// <summary>
			/// Disposes the resources associated with a streamable object on the server.
			/// </summary>
			/// <param name="result_id"></param>
			/// <param name="streamable_object_id"></param>
			/// <remarks>
			/// This would typically be called when either of the following situations
			/// occured - the IBlob is closed/disposed/finalized, the InputStream is 
			/// closes/finalized.
			/// <para>
			/// It's very important that the server resources for a streamable object 
			/// is released.
			/// </para>
			/// </remarks>
			/// <returns>
			/// Returns the dispatch id key for the response from the server.
			/// </returns>
			internal int DisposeStreamableObject(int result_id, long streamable_object_id) {
				lock (this) {
					int dispatch_id = NextDispatchId();
					com_data.Write(ProtocolConstants.DISPOSE_STREAMABLE_OBJECT);
					com_data.Write(dispatch_id);
					com_data.Write(result_id);
					com_data.Write(streamable_object_id);
					FlushCommand();

					return dispatch_id;
				}
			}

			/// <summary>
			/// Sends close command to server.
			/// </summary>
			/// <returns></returns>
			internal int SendCloseCommand() {
				lock (this) {
					int dispatch_id = NextDispatchId();
					com_data.Write(ProtocolConstants.CLOSE);
					com_data.Write(dispatch_id);
					FlushCommand();

					return dispatch_id;
				}
			}


			// ---------- Server Read methods ----------

			/// <summary>
			/// Listens for commands from the server.
			/// </summary>
			/// <remarks>
			/// When received puts the command on the dispatch list.
			/// </remarks>
			private void run() {

				try {
					while (!thread_closed) {

						// Block until next command received from server.
						byte[] buf = remote_interface.NextCommandFromServer(0);
						int dispatch_id = ByteBuffer.ReadInt4(buf, 0);

						if (dispatch_id == -1) {
							// This means a trigger or a ping or some other server side event.
							ProcessEvent(buf);
						}

						lock (commands_list) {
							// Add this command to the commands list
							commands_list.Add(new ServerCommand(dispatch_id, buf));
							// Notify any threads waiting on it.
							Monitor.PulseAll(commands_list);
						}

					} // while(true)

				} catch (IOException e) {
					//      System.err.println("Connection Thread closed because of IOException");
					//      e.printStackTrace();
				} finally {
					// Invalidate this object when the thread finishes.
					Object old_commands_list = commands_list;
					lock (old_commands_list) {
						commands_list = null;
						Monitor.PulseAll(old_commands_list);
					}
				}

			}

			/// <summary>
			/// Processes a server side event.
			/// </summary>
			/// <param name="buf"></param>
			private void ProcessEvent(byte[] buf) {
				int ev = ByteBuffer.ReadInt4(buf, 4);
				if (ev == ProtocolConstants.PING) {
					// Ignore ping events, they only sent by server to see if we are
					// alive.  Ping back?
				} else if (ev == ProtocolConstants.DATABASE_EVENT) {
					// A database event that is passed to the IDatabaseCallBack...
					MemoryStream bin = new MemoryStream(buf, 8, buf.Length - 8);
					BinaryReader din = new BinaryReader(bin);

					int event_type = din.ReadInt32();
					String event_msg = din.ReadString();
					remote_interface.database_call_back.OnDatabaseEvent(event_type, event_msg);
				}
					//      else if (event == SERVER_REQUEST) {
					//        // A server request that is passed to the IDatabaseCallBack...
					//        ByteArrayInputStream bin =
					//                              new ByteArrayInputStream(buf, 8, buf.length - 8);
					//        DataInputStream din = new DataInputStream(bin);
					//
					//        int command = din.readInt();        // Currently ignored
					//        long stream_id = din.readLong();
					//        int length = din.readInt();
					//        database_call_back.streamableObjectRequest(stream_id, length);
					//      }
				else {
					Console.Error.WriteLine("[RemoteDatabaseInterface] " +
									 "Received unrecognised server side event: " + ev);
				}
			}

			public void Start() {
				thread.Start();
			}
		}


		/// <summary>
		/// Represents the data input a command from the server.
		/// </summary>
		sealed class ServerCommand {

			private int dispatch_id;
			private byte[] buf;

			internal ServerCommand(int dispatch_id, byte[] buf) {
				this.dispatch_id = dispatch_id;
				this.buf = buf;
			}

			public int DispatchId {
				get { return dispatch_id; }
			}

			public byte[] Buffer {
				get { return buf; }
			}

			public MemoryStream GetInputStream() {
				return new MemoryStream(buf, 4, buf.Length - 4);
			}
		}
	}
}