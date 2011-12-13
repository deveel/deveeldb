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
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;

using Deveel.Data.Protocol;
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
		private ConnectionThread connectionThread;

		/// <summary>
		/// A <see cref="DatabaseEventCallback"/> implementation that is notified of 
		/// all events that are received from the database.
		/// </summary>
		private DatabaseEventCallback databaseCallback;

		/// <summary>
		/// The initial database for the connection.
		/// </summary>
		private readonly string initialDatabase;

		/// <summary>
		/// The version of the server this interface is connected to.
		/// </summary>
		private Version serverVersion = new Version();

		/// <summary>
		/// Constructs a new <see cref="RemoteDatabaseInterface"/> which points to the
		/// given initial database.
		/// </summary>
		/// <param name="database">The initial database for the connection.</param>
		protected RemoteDatabaseInterface(string database) {
			initialDatabase = database;
		}

		~RemoteDatabaseInterface() {
			Dispose(false);
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
		private static void LogException(Exception e) {
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
		protected abstract void SendCommand(byte[] command, int offset, int length);

		/// <summary>
		/// Blocks until the next command is received from the server.
		/// </summary>
		/// <param name="timeout"></param>
		/// <remarks>
		/// The way this is implemented is network layer dependant.
		/// </remarks>
		/// <returns></returns>
		protected abstract byte[] ReceiveCommand(int timeout);

		/// <summary>
		/// Closes the connection.
		/// </summary>
		protected abstract void CloseConnection();


		/// <inheritdoc/>
		public bool Login(string defaultSchema, string user, string password, DatabaseEventCallback callback) {
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
				output.Write(initialDatabase);
				byte[] arr = bout.ToArray();
				SendCommand(arr, 0, arr.Length);

				byte[] response = ReceiveCommand(0);

				int ack = ByteBuffer.ReadInt4(response, 0);
				if (ack == ProtocolConstants.Acknowledgement) {
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
					output.Write(defaultSchema);
					output.Write(user);
					output.Write(password);
					arr = bout.ToArray();
					SendCommand(arr, 0, arr.Length);

					response = ReceiveCommand(0);
					int result = ByteBuffer.ReadInt4(response, 0);
					if (result == ProtocolConstants.UserAuthenticationPassed) {
						// Set the callback,
						databaseCallback = callback;

						// User authentication passed so we successfully logged input now.
						connectionThread = new ConnectionThread(this);
						connectionThread.Start();
						return true;

					}
					if (result == ProtocolConstants.UserAuthenticationFailed)
						throw new DataException("User Authentication failed.");
					if (result == ProtocolConstants.DatabaseNotFound)
						throw new DataException("The database specified was not found.");

					throw new DataException("Unexpected response.");
				}
				if (ack == ProtocolConstants.DatabaseNotFound)
					throw new DataException("The database was not found.");

				throw new DataException("No acknowledgement received from server.");
			} catch (IOException e) {
				LogException(e);
				throw new DataException("IOException: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public void PushStreamableObjectPart(ReferenceType type, long objectId, long objectLength, byte[] buf, long offset, int length) {
			try {
				// Push the object part
				int dispatchId = connectionThread.PushStreamableObjectPart(type, objectId, objectLength, buf, offset, length);
				// Get the response
				ServerCommand command = connectionThread.ReceiveCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatchId);
				// If command == null then we timed output
				if (command == null)
					throw new DataException("Query timed output after " + DeveelDbConnection.QUERY_TIMEOUT + " seconds.");

				BinaryReader reader = new BinaryReader(command.GetInputStream());
				int status = reader.ReadInt32();

				// If failed report the error.
				if (status == ProtocolConstants.Failed)
					throw new DataException("Push object failed: " + reader.ReadString());
			} catch (IOException e) {
				LogException(e);
				throw new DataException("IO Error: " + e.Message);
			}

		}

		public void ChangeDatabase(string database) {
			try {
				// Change the current database
				int dispatchId = connectionThread.ChangeDatabase(database);

				// get the response
				ServerCommand command = connectionThread.ReceiveCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatchId);
				if (command == null)
					throw new DataException("Query timed output after " + DeveelDbConnection.QUERY_TIMEOUT + " seconds.");

				BinaryReader reader = new BinaryReader(command.GetInputStream());
				int status = reader.ReadInt32();
				if (status == ProtocolConstants.Failed)
					throw new DataException("Change database failed: " + reader.ReadString());

				if (status == ProtocolConstants.DatabaseNotFound)
					throw new DataException("The database '" + database + "' was not found on the server.");
			} catch(IOException e) {
				LogException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public IQueryResponse ExecuteQuery(SqlQuery sql) {
			try {
				// Execute the command
				int dispatchId = connectionThread.ExecuteQuery(sql);

				// Get the response
				ServerCommand command = connectionThread.ReceiveCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatchId);
				// If command == null then we timed output
				if (command == null)
					throw new DataException("Query timed output after " + DeveelDbConnection.QUERY_TIMEOUT + " seconds.");

				BinaryReader input = new BinaryReader(command.GetInputStream());

				// Query response protocol...
				int status = input.ReadInt32();
				if (status == ProtocolConstants.Success) {
					int resultId = input.ReadInt32();
					int queryTime = input.ReadInt32();
					int rowCount = input.ReadInt32();
					int colCount = input.ReadInt32();
					ColumnDescription[] col_list = new ColumnDescription[colCount];
					for (int i = 0; i < colCount; ++i) {
						col_list[i] = ColumnDescription.ReadFrom(input);
					}

					return new QueryResponseImpl(resultId, queryTime, colCount, rowCount, col_list);
				} 
				if (status == ProtocolConstants.Exception) {
					int dbCode = input.ReadInt32();
					string message = input.ReadString();
					string stack_trace = input.ReadString();
					throw new DbDataException(message, null, dbCode, stack_trace);
				}
				if (status == ProtocolConstants.AuthenticationError) {
					// Means we could perform the command because user doesn't have enough
					// rights.
					string accessType = input.ReadString();
					string tableName = input.ReadString();
					throw new DataException("User doesn't have enough privs to " + accessType + " table " + tableName);
				}

				throw new DataException("Illegal response code from server.");
			} catch (IOException e) {
				LogException(e);
				throw new DataException("IO Error: " + e.Message);
			}

		}

		/// <inheritdoc/>
		public ResultPart GetResultPart(int resultId, int startRow, int countRows) {
			try {
				// Get the first few rows of the result..
				int dispatchId = connectionThread.GetResultPart(resultId, startRow, countRows);

				// Get the response
				ServerCommand command = connectionThread.ReceiveCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatchId);
				// If command == null then we timed output
				if (command == null)
					throw new DataException("Downloading result part timed output after " + DeveelDbConnection.QUERY_TIMEOUT +
					                        " seconds.");

				// Wrap around a DataInputStream
				BinaryReader reader = new BinaryReader(command.GetInputStream());
				int status = reader.ReadInt32();

				if (status == ProtocolConstants.Success) {
					// Return the contents of the response.
					int colCount = reader.ReadInt32();
					int size = countRows * colCount;
					ResultPart list = new ResultPart(size);
					for (int i = 0; i < size; ++i) {
						list.Add(ObjectTransfer.ReadFrom(reader));
					}
					return list;
				}

				if (status == ProtocolConstants.Exception) {
					int dbCode = reader.ReadInt32();
					string message = reader.ReadString();
					string stackTrace = reader.ReadString();
					throw new DbDataException(message, message, dbCode, stackTrace);
				}

				throw new DataException("Illegal response code from server.");
			} catch (IOException e) {
				LogException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public void DisposeResult(int resultId) {
			try {
				int dispatchId = connectionThread.DisposeResult(resultId);

				// Get the response
				ServerCommand command = connectionThread.ReceiveCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatchId);
				// If command == null then we timed output
				if (command == null) {
					throw new DataException("Dispose result timed output after " +
										   DeveelDbConnection.QUERY_TIMEOUT + " seconds.");
				}

				// Check the dispose was successful.
				BinaryReader reader = new BinaryReader(command.GetInputStream());
				int status = reader.ReadInt32();

				// If failed report the error.
				if (status == ProtocolConstants.Failed)
					throw new DataException("Dispose failed: " + reader.ReadString());
			} catch (IOException e) {
				LogException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public byte[] GetStreamableObjectPart(int resultId, long streamableObjectId, long offset, int len) {
			try {
				int dispatchId = connectionThread.GetStreamableObjectPart(resultId, streamableObjectId, offset, len);
				ServerCommand command = connectionThread.ReceiveCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatchId);

				// If command == null then we timed output
				if (command == null)
					throw new DataException("GetStreamableObjectPart timed output after " + DeveelDbConnection.QUERY_TIMEOUT +
					                        " seconds.");

				BinaryReader reader = new BinaryReader(command.GetInputStream());
				int status = reader.ReadInt32();

				if (status == ProtocolConstants.Success) {
					// Return the contents of the response.
					int contentsSize = reader.ReadInt32();
					byte[] buf = new byte[contentsSize];
					reader.Read(buf, 0, contentsSize);
					return buf;
				}

				if (status == ProtocolConstants.Exception) {
					int dbCode = reader.ReadInt32();
					string message = reader.ReadString();
					string stackTrace = reader.ReadString();
					throw new DbDataException(message, message, dbCode, stackTrace);
				}

				throw new DataException("Illegal response code from server.");
			} catch (IOException e) {
				LogException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public void DisposeStreamableObject(int resultId, long streamableObjectId) {
			try {
				int dispatchId = connectionThread.DisposeStreamableObject(resultId, streamableObjectId);
				ServerCommand command = connectionThread.ReceiveCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatchId);

				// If command == null then we timed output
				if (command == null)
					throw new DataException("DisposeStreamableObject timed output after " +
					                        DeveelDbConnection.QUERY_TIMEOUT + " seconds.");

				BinaryReader reader = new BinaryReader(command.GetInputStream());
				int status = reader.ReadInt32();

				// If failed report the error.
				if (status == ProtocolConstants.Failed)
					throw new DataException("Dispose failed: " + reader.ReadString());
			} catch (IOException e) {
				LogException(e);
				throw new DataException("IO Error: " + e.Message);
			}
		}

		/// <inheritdoc/>
		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				try {
					int dispatchId = connectionThread.SendCloseCommand();
					//      // Get the response
					//      ServerCommand command =
					//            connection_thread.ReceiveCommand(DeveelDbConnection.QUERY_TIMEOUT, dispatchId);
					CloseConnection();
				} catch (IOException e) {
					LogException(e);
					throw new DataException("IO Error: " + e.Message);
				}
			}
		}

		private class QueryResponseImpl : IQueryResponse {
			private readonly int resultId;
			private readonly int queryTime;
			private readonly int colCount;
			private readonly int rowCount;
			private readonly ColumnDescription[] colList;

			public QueryResponseImpl(int resultId, int queryTime, int colCount, int rowCount, ColumnDescription[] colList) {
				this.resultId = resultId;
				this.colList = colList;
				this.rowCount = rowCount;
				this.colCount = colCount;
				this.queryTime = queryTime;
			}

			public int ResultId {
				get { return resultId; }
			}

			public int QueryTimeMillis {
				get { return queryTime; }
			}

			public int RowCount {
				get { return rowCount; }
			}

			public int ColumnCount {
				get { return colCount; }
			}

			public ColumnDescription GetColumnDescription(int n) {
				return colList[n];
			}

			public string Warnings {
				get { return ""; }
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
			private readonly RemoteDatabaseInterface remoteInterface;
			/// <summary>
			/// The command to write output to the server.
			/// </summary>
			private MemoryStream commandStream;
			private BinaryWriter commandWriter;

			/// <summary>
			/// Running dispatch id values which we use as a unique key.
			/// </summary>
			private int runningDispatchId = 1;

			/// <summary>
			/// Set to true when the thread is closed.
			/// </summary>
			private readonly bool threadClosed;

			/// <summary>
			/// The list of commands received from the server that are pending to be
			/// processed (ServerCommand).
			/// </summary>
			private List<ServerCommand> commands;

			/// <summary>
			/// Constructs the connection thread.
			/// </summary>
			/// <param name="remoteInterface"></param>
			internal ConnectionThread(RemoteDatabaseInterface remoteInterface) {
				this.remoteInterface = remoteInterface;
				thread = new Thread(new ThreadStart(Run));
				thread.IsBackground = true;
				thread.Name = "RemoteDatabaseInterface::Connection Thread";
				commandStream = new MemoryStream();
				commandWriter = new BinaryWriter(commandStream, Encoding.Unicode);

				commands = new List<ServerCommand>();
				threadClosed = false;
			}

			// ---------- Utility ----------

			/// <summary>
			/// Returns a unique dispatch id number for a command.
			/// </summary>
			/// <returns></returns>
			private int NextDispatchId() {
				return runningDispatchId++;
			}

			/// <summary>
			/// Blocks until a response from the server has been received with the 
			/// given <paramref name="dispatchId"> dispatch id</paramref>.
			/// </summary>
			/// <param name="dispatchId"></param>
			/// <param name="timeout">The maximum time, in milliseconds, to wait for the 
			/// command to be received from the remote endpoint.</param>
			/// <returns>
			/// Returns a command received from the remote end point or <b>null</b> if the
			/// timeout expired without received any data.
			/// </returns>
			public ServerCommand ReceiveCommand(int timeout, int dispatchId) {
				DateTime timeIn = DateTime.Now;
				DateTime timeOutHigh = timeIn + new TimeSpan(((long)timeout * 1000) * TimeSpan.TicksPerMillisecond);

				lock (commands) {
					if (commands == null)
						throw new DataException("Connection to server closed");

					while (true) {
						for (int i = 0; i < commands.Count; ++i) {
							ServerCommand command = commands[i];
							if (command.DispatchId == dispatchId) {
								commands.RemoveAt(i);
								return command;
							}
						}

						// Return null if we haven't received a response input the timeout
						// period.
						if (timeout != 0 &&
							DateTime.Now > timeOutHigh) {
							return null;
						}

						// Wait a second.
						try {
							Monitor.Wait(commands, 1000);
						} catch (ThreadInterruptedException) { /* ignore */ }

					} // while (true)
				} // lock
			}


			// ---------- Server request methods ----------

			/// <summary>
			/// Flushes the command input 'commandStream' to the server.
			/// </summary>
			private void FlushCommand() {
				lock (this) {
					// We flush the size of the command string followed by the command
					// itself to the server.  This format allows us to implement a simple
					// non-blocking command parser on the server.
					commandWriter.Flush();
					remoteInterface.SendCommand(commandStream.ToArray(), 0, (int)commandStream.Length);
					commandStream = new MemoryStream();
					commandWriter = new BinaryWriter(commandStream, Encoding.Unicode);
				}
			}

			public int ChangeDatabase(string database) {
				lock (this) {
					int dispatchId = NextDispatchId();
					commandWriter.Write(ProtocolConstants.ChangeDatabase);
					commandWriter.Write(dispatchId);
					commandWriter.Write(database);
					FlushCommand();

					return dispatchId;
				}
			}

			/// <summary>
			/// Pushes a part of a streamable object onto the server.
			/// </summary>
			/// <param name="type"></param>
			/// <param name="objectId"></param>
			/// <param name="objectLength"></param>
			/// <param name="buf"></param>
			/// <param name="offset"></param>
			/// <param name="length"></param>
			/// <remarks>
			/// Used input preparation to executing queries containing large objects.
			/// </remarks>
			/// <returns></returns>
			public int PushStreamableObjectPart(ReferenceType type, long objectId, long objectLength, byte[] buf, long offset, int length) {
				lock (this) {
					int dispatch_id = NextDispatchId();
					commandWriter.Write(ProtocolConstants.PushStreamableObjectPart);
					commandWriter.Write(dispatch_id);
					commandWriter.Write((byte)type);
					commandWriter.Write(objectId);
					commandWriter.Write(objectLength);
					commandWriter.Write(length);
					commandWriter.Write(buf, 0, length);
					commandWriter.Write(offset);
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
			public int ExecuteQuery(SqlQuery sql) {
				lock (this) {
					int dispatchId = NextDispatchId();
					commandWriter.Write(ProtocolConstants.Query);
					commandWriter.Write(dispatchId);
					sql.WriteTo(commandWriter);
					FlushCommand();

					return dispatchId;
				}
			}

			/// <summary>
			/// Releases the server side resources associated with a given command 
			/// key returned by the server.
			/// </summary>
			/// <param name="resultId"></param>
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
			public int DisposeResult(int resultId) {
				lock (this) {
					int dispatchId = NextDispatchId();
					commandWriter.Write(ProtocolConstants.DisposeResult);
					commandWriter.Write(dispatchId);
					commandWriter.Write(resultId);
					FlushCommand();

					return dispatchId;
				}
			}

			/// <summary>
			/// Requests a part of a result of a command.
			/// </summary>
			/// <param name="resultId">The identifier of the result to download from: this
			/// is generated by the <c>command</c> command.</param>
			/// <param name="rowNumber">The row to download from.</param>
			/// <param name="rowCount">The number of rows to download.</param>
			/// <remarks>
			/// This is used to download a part of a result set from the server. 
			/// This will generate an error if the <paramref name="resultId"/> is 
			/// invalid or has previously been disposed.
			/// </remarks>
			/// <returns>
			/// Returns the dispatch id key for the response from the server.
			/// </returns>
			public int GetResultPart(int resultId, int rowNumber, int rowCount) {
				lock (this) {
					int dispatchId = NextDispatchId();
					commandWriter.Write(ProtocolConstants.ResultSection);
					commandWriter.Write(dispatchId);
					commandWriter.Write(resultId);
					commandWriter.Write(rowNumber);
					commandWriter.Write(rowCount);
					FlushCommand();

					return dispatchId;
				}
			}

			/// <summary>
			/// Requests a part of an open <see cref="StreamableObject"/> channel.
			/// </summary>
			/// <param name="resultId"></param>
			/// <param name="streamableObjectId">The identifier of the object to
			/// download: this is returned by <see cref="StreamableObject.Identifier"/></param>
			/// <param name="offset"></param>
			/// <param name="length"></param>
			/// <remarks>
			/// This is used to download a section of a large object, such as a 
			/// <see cref="IRef"/>.
			/// </remarks>
			/// <returns>
			/// Returns the dispatch id key for the response from the server.
			/// </returns>
			internal int GetStreamableObjectPart(int resultId, long streamableObjectId, long offset, int length) {
				lock (this) {
					int dispatchId = NextDispatchId();
					commandWriter.Write(ProtocolConstants.StreamableObjectSection);
					commandWriter.Write(dispatchId);
					commandWriter.Write(resultId);
					commandWriter.Write(streamableObjectId);
					commandWriter.Write(offset);
					commandWriter.Write(length);
					FlushCommand();

					return dispatchId;
				}
			}

			/// <summary>
			/// Disposes the resources associated with a streamable object on the server.
			/// </summary>
			/// <param name="resultId"></param>
			/// <param name="streamableObjectId"></param>
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
			public int DisposeStreamableObject(int resultId, long streamableObjectId) {
				lock (this) {
					int dispatchId = NextDispatchId();
					commandWriter.Write(ProtocolConstants.DisposeStreamableObject);
					commandWriter.Write(dispatchId);
					commandWriter.Write(resultId);
					commandWriter.Write(streamableObjectId);
					FlushCommand();

					return dispatchId;
				}
			}

			/// <summary>
			/// Sends close command to server.
			/// </summary>
			/// <returns></returns>
			public int SendCloseCommand() {
				lock (this) {
					int dispatchId = NextDispatchId();
					commandWriter.Write(ProtocolConstants.Close);
					commandWriter.Write(dispatchId);
					FlushCommand();

					return dispatchId;
				}
			}


			// ---------- Server Read methods ----------

			/// <summary>
			/// Listens for commands from the server.
			/// </summary>
			/// <remarks>
			/// When received puts the command on the dispatch list.
			/// </remarks>
			private void Run() {
				try {
					while (!threadClosed) {
						// Block until next command received from server.
						byte[] buf = remoteInterface.ReceiveCommand(0);
						int dispatchId = ByteBuffer.ReadInt4(buf, 0);

						if (dispatchId == -1)
							// This means a trigger or a ping or some other server side event.
							ProcessEvent(buf);

						lock (commands) {
							// Add this command to the commands list
							commands.Add(new ServerCommand(dispatchId, buf));
							// Notify any threads waiting on it.
							Monitor.PulseAll(commands);
						}

					} // while(true)
				} catch (IOException e) {
					//      Console.Error.WriteLine("Connection Thread closed because of IOException");
					//      Console.Error.WriteLine(e.StackTrace);
				} finally {
					// Invalidate this object when the thread finishes.
					object oldCommandsList = commands;
					lock (oldCommandsList) {
						commands = null;
						Monitor.PulseAll(oldCommandsList);
					}
				}

			}

			/// <summary>
			/// Processes a server side event.
			/// </summary>
			/// <param name="buf"></param>
			private void ProcessEvent(byte[] buf) {
				int ev = ByteBuffer.ReadInt4(buf, 4);
				if (ev == ProtocolConstants.Ping) {
					// Ignore ping events, they only sent by server to see if we are
					// alive.  Ping back?
				} else if (ev == ProtocolConstants.DatabaseEvent) {
					// A database event that is passed to the IDatabaseCallBack...
					MemoryStream stream = new MemoryStream(buf, 8, buf.Length - 8);
					BinaryReader reader = new BinaryReader(stream);

					int eventType = reader.ReadInt32();
					string eventMsg = reader.ReadString();
					remoteInterface.databaseCallback(eventType, eventMsg);
				} else {
					Console.Error.WriteLine("[RemoteDatabaseInterface] Received unrecognised server side event: " + ev);
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
			private readonly int dispatchId;
			private readonly byte[] buf;

			internal ServerCommand(int dispatchId, byte[] buf) {
				this.dispatchId = dispatchId;
				this.buf = buf;
			}

			public int DispatchId {
				get { return dispatchId; }
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