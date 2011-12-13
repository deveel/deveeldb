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

using Deveel.Data.Client;
using Deveel.Data.Control;
using Deveel.Data.Server;
using Deveel.Data.Util;
using Deveel.Diagnostics;

namespace Deveel.Data.Protocol {
	/// <summary>
	/// This processes _queries from a client and dispatches the _queries 
	/// to the database.
	/// </summary>
	/// <remarks>
	/// This is a state based class. There is a single processor for each client 
	/// connected. This class is designed to be flexible enough to handle packet 
	/// based protocols as well as stream based protocols.
	/// </remarks>
	public abstract class Processor : IDisposable {
		/// <summary>
		/// The version of the server protocol.
		/// </summary>
		private const int ServerVersion = 1;

		/// <summary>
		/// The current state we are in.
		/// </summary>
		private ClientConnectionState state;

		/// <summary>
		/// Number of authentications tried.
		/// </summary>
		private int authenticationTries;

		private readonly string hostString;

		private readonly DbController controller;
		private readonly Dictionary<string, DatabaseInterface> dbInterfaces;
		private DatabaseInterface dbInterface;

		private Database database;

		protected Processor(DbController controller, string hostString) {
			this.hostString = hostString;
			dbInterfaces = new Dictionary<string, DatabaseInterface>();
			this.controller = controller;
			state = 0;
			authenticationTries = 0;
			dbCallback = OnDatabaseEvent;
		}

		private void OnDatabaseEvent(int eventType, String eventMessage) {
			try {
				// Format the call back and send the event.
				MemoryStream bout = new MemoryStream();
				BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);
				dout.Write(eventType);
				dout.Write(eventMessage);
				SendEvent(bout.ToArray());
			} catch (IOException e) {
				controller.Debug.Write(DebugLevel.Error, this, "IO Error: " + e.Message);
				controller.Debug.WriteException(e);
			}
		}


		/// <summary>
		/// The database call back method that sends database events back to the client.
		/// </summary>
		private readonly DatabaseEventCallback dbCallback;

		private bool ChangeDatabaseInterface(string databaseName) {
			if (!controller.DatabaseExists(databaseName))
				return false;

			DatabaseInterface dbi;
			if (!dbInterfaces.TryGetValue(databaseName, out dbi)) {
				dbi = new DatabaseInterface(controller, databaseName, hostString);
				dbInterfaces[databaseName] = dbi;
			}

			database = controller.GetDatabase(databaseName);
			dbInterface = dbi;
			return true;
		}

		/// <summary>
		/// Processes a single Query from the client.
		/// </summary>
		/// <param name="command"></param>
		/// <returns>
		/// Returns a byte array and the response is written out as a byte array, or
		/// <b>null</b> if the connection has been closed.
		/// </returns>
		protected byte[] ProcessCommand(byte[] command) {
			if (state == ClientConnectionState.Closed) {
				// State 0 means we looking for the header...
				BinaryReader reader = new BinaryReader(new MemoryStream(command), Encoding.ASCII);
				/*
				int magic = ByteBuffer.ReadInt4(Query, 0);
				// The driver version number
				int maj_ver = ByteBuffer.ReadInt4(Query, 4);
				int min_ver = ByteBuffer.ReadInt4(Query, 8);
				*/
				reader.ReadInt32();		// magic
				reader.ReadInt32();		// server major version
				reader.ReadInt32();		// server minor version

				string databaseName = reader.ReadString();

				if (!ChangeDatabaseInterface(databaseName))
					return Single(ProtocolConstants.DatabaseNotFound);

				Version version = Assembly.GetExecutingAssembly().GetName().Version;

				byte[] ackCommand = new byte[4 + 1 + 4 + 4 + 1];
				// Send back an acknowledgement and the version number of the server
				ByteBuffer.WriteInteger(ProtocolConstants.Acknowledgement, ackCommand, 0);
				ackCommand[4] = 1;
				ByteBuffer.WriteInteger(version.Major, ackCommand, 5);
				ByteBuffer.WriteInteger(version.Minor, ackCommand, 9);
				ackCommand[13] = 0;

				// Set to the next state.
				state = ClientConnectionState.NotAuthenticated;

				// Return the acknowledgement
				return ackCommand;
			}

			if (state == ClientConnectionState.NotAuthenticated) {
				// State 4 means we looking for username and password...
				MemoryStream input = new MemoryStream(command);
				BinaryReader reader = new BinaryReader(input, Encoding.ASCII);
				string defaultSchema = reader.ReadString();
				string username = reader.ReadString();
				string password = reader.ReadString();

				try {
					if (!dbInterface.Login(defaultSchema, username, password, dbCallback)) {
						// Close after 12 tries.
						if (authenticationTries >= 12) {
							Close();
						} else {
							++authenticationTries;
							return Single(ProtocolConstants.UserAuthenticationFailed);
						}
					} else {
						state = ClientConnectionState.Processing;
						return Single(ProtocolConstants.UserAuthenticationPassed);
					}
				} catch (DataException) {

				}

				return null;
			}

			if (state == ClientConnectionState.Processing)
				// Process the query
				return ProcessQuery(command);

			throw new Exception("Illegal state: " + state);
		}

		/// <summary>
		/// Returns the state of the connection.
		/// </summary>
		/// <remarks>
		/// 0 = not logged in yet.  1 = logged in.
		/// </remarks>
		protected ClientConnectionState ClientState {
			get { return state; }
		}


		/// <summary>
		/// Returns a single 4 byte array with the given int encoded into it.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		private static byte[] Single(int val) {
			byte[] buf = new byte[4];
			ByteBuffer.WriteInteger(val, buf, 0);
			return buf;
		}

		/// <summary>
		/// Creates a response that represents a data exception failure.
		/// </summary>
		/// <param name="dispatchId"></param>
		/// <param name="e"></param>
		/// <returns></returns>
		private static byte[] Exception(int dispatchId, DataException e) {
			int code = /* TODO: e.ErrorCode */ -1;
			string msg = e.Message;
			if (String.IsNullOrEmpty(msg))
				msg = "NULL exception message";

			string server_msg = "";
			string stack_trace = "";

			if (e is DbDataException) {
				DbDataException me = (DbDataException)e;
				server_msg = me.ServerErrorMessage;
				stack_trace = me.ServerErrorStackTrace;
			} else {
				stack_trace = e.StackTrace;
			}

			MemoryStream output = new MemoryStream();
			BinaryWriter writer = new BinaryWriter(output, Encoding.Unicode);
			writer.Write(dispatchId);
			writer.Write(ProtocolConstants.Exception);
			writer.Write(code);
			writer.Write(msg);
			writer.Write(stack_trace);

			return output.ToArray();
		}

		/// <summary>
		/// Creates a response that indicates a simple success of an operation 
		/// with the given dispatch id.
		/// </summary>
		/// <param name="dispatchId"></param>
		/// <returns></returns>
		private static byte[] SimpleSuccess(int dispatchId) {
			byte[] buf = new byte[8];
			ByteBuffer.WriteInteger(dispatchId, buf, 0);
			ByteBuffer.WriteInteger(ProtocolConstants.Success, buf, 4);
			return buf;
		}

		/// <summary>
		/// Processes a query on the byte[] array and returns the result.
		/// </summary>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] ProcessQuery(byte[] command) {
			byte[] result;

			// The first int is the Query.
			int ins = ByteBuffer.ReadInt4(command, 0);

			// Otherwise must be a dispatch type request.
			// The second is the dispatch id.
			int dispatchId = ByteBuffer.ReadInt4(command, 4);

			if (dispatchId == -1)
				throw new Exception("Special case dispatch id of -1 in query");

			if (ins == ProtocolConstants.ChangeDatabase) {
				result = ChangeDatabase(dispatchId, command);
			} else if (ins == ProtocolConstants.ResultSection) {
				result = ResultSection(dispatchId, command);
			} else if (ins == ProtocolConstants.Query) {
				result = QueryCommand(dispatchId, command);
			} else if (ins == ProtocolConstants.PushStreamableObjectPart) {
				result = PushStreamableObjectPart(dispatchId, command);
			} else if (ins == ProtocolConstants.DisposeResult) {
				result = DisposeResult(dispatchId, command);
			} else if (ins == ProtocolConstants.StreamableObjectSection) {
				result = StreamableObjectSection(dispatchId, command);
			} else if (ins == ProtocolConstants.DisposeStreamableObject) {
				result = DisposeStreamableObject(dispatchId, command);
			} else if (ins == ProtocolConstants.Close) {
				Close();
				result = null;
			} else {
				throw new Exception("Query (" + ins + ") not understood.");
			}

			return result;

		}

		/// <summary>
		///  Disposes of this processor.
		/// </summary>
		protected void Dispose(bool disposing) {
			if (disposing) {
				try {
					if (dbInterfaces.Count > 0) {
						foreach (DatabaseInterface databaseInterface in dbInterfaces.Values) {
							databaseInterface.Dispose();
						}
					}
					dbInterfaces.Clear();
				} catch (Exception e) {
					controller.Debug.WriteException(DebugLevel.Error, e);
				}
			}
		}


		// ---------- Primitive _queries ----------

		private byte [] ChangeDatabase(int dispatchId, byte[] command) {
			// Read the query from the Query.
			MemoryStream input = new MemoryStream(command, 8, command.Length - 8);
			BinaryReader reader = new BinaryReader(input, Encoding.Unicode);

			string databaseName = reader.ReadString();

			try {
				dbInterface.ChangeDatabase(databaseName);
				state = ClientConnectionState.NotAuthenticated;
				return SimpleSuccess(dispatchId);
			} catch (DataException e) {
				return Exception(dispatchId, e);
			}
		}

		/// <summary>
		/// Executes a query and returns the header for the result in the response.
		/// </summary>
		/// <param name="dispatchId">The number we need to respond with.</param>
		/// <param name="command"></param>
		/// <remarks>
		/// This keeps track of all result sets because sections of the result are 
		/// later queries via the <see cref="ProtocolConstants.ResultSection"/>
		/// Query.
		/// </remarks>
		/// <returns></returns>
		private byte[] QueryCommand(int dispatchId, byte[] command) {
			// Read the query from the Query.
			MemoryStream input = new MemoryStream(command, 8, command.Length - 8);
			BinaryReader reader = new BinaryReader(input, Encoding.Unicode);
			SqlQuery query = SqlQuery.ReadFrom(reader);

			try {
				// Do the query
				IQueryResponse response = dbInterface.ExecuteQuery(query);

				// Prepare the stream to output the response to,
				MemoryStream output = new MemoryStream();
				BinaryWriter writer = new BinaryWriter(output, Encoding.Unicode);

				writer.Write(dispatchId);
				writer.Write(ProtocolConstants.Success);

				// The response sends the result id, the time the query took, the
				// total row count, and description of each column in the result.
				writer.Write(response.ResultId);
				writer.Write(response.QueryTimeMillis);
				writer.Write(response.RowCount);
				int colCount = response.ColumnCount;
				writer.Write(colCount);
				for (int i = 0; i < colCount; ++i) {
					response.GetColumnDescription(i).WriteTo(writer);
				}
				writer.Flush();
				return output.ToArray();
			} catch (DataException e) {
				//      debug.writeException(e);
				return Exception(dispatchId, e);
			}

		}


		/// <summary>
		/// Pushes a part of a streamable object onto the server.
		/// </summary>
		/// <param name="dispatchId">The number we need to respond with.</param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] PushStreamableObjectPart(int dispatchId, byte[] command) {
			ReferenceType type = (ReferenceType) command[8];
			long objectId = ByteBuffer.ReadInt8(command, 9);
			long objectLength = ByteBuffer.ReadInt8(command, 17);
			int length = ByteBuffer.ReadInt4(command, 25);
			byte[] obBuf = new byte[length];
			Array.Copy(command, 29, obBuf, 0, length);
			long offset = ByteBuffer.ReadInt8(command, 29 + length);

			try {
				// Pass this through to the underlying database interface.
				dbInterface.PushStreamableObjectPart(type, objectId, objectLength, obBuf, offset, length);

				// Return operation success.
				return SimpleSuccess(dispatchId);
			} catch (DataException e) {
				return Exception(dispatchId, e);
			}
		}


		/// <summary>
		/// Responds with a part of the result set of a query made via the 
		/// <see cref="ProtocolConstants.Query"/> Query.
		/// </summary>
		/// <param name="dispatchId">The number we need to respond with.</param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] ResultSection(int dispatchId, byte[] command) {
			int resultId = ByteBuffer.ReadInt4(command, 8);
			int rowNumber = ByteBuffer.ReadInt4(command, 12);
			int rowCount = ByteBuffer.ReadInt4(command, 16);

			try {
				// Get the result part...
				ResultPart block = dbInterface.GetResultPart(resultId, rowNumber, rowCount);

				MemoryStream output = new MemoryStream();
				BinaryWriter writer = new BinaryWriter(output, Encoding.Unicode);

				writer.Write(dispatchId);
				writer.Write(ProtocolConstants.Success);

				// Send the contents of the result set.
				// HACK - Work out column count by dividing number of entries in block
				//   by number of rows.
				int colCount = block.Count / rowCount;
				writer.Write(colCount);
				int bsize = block.Count;
				for (int index = 0; index < bsize; ++index) {
					ObjectTransfer.WriteTo(writer, block[index]);
				}

				writer.Flush();
				return output.ToArray();
			} catch (DataException e) {
				return Exception(dispatchId, e);
			}
		}

		/// <summary>
		/// Returns a section of a streamable object.
		/// </summary>
		/// <param name="dispatchId">The number we need to respond with.</param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] StreamableObjectSection(int dispatchId, byte[] command) {
			int resultId = ByteBuffer.ReadInt4(command, 8);
			long streamableObjectId = ByteBuffer.ReadInt8(command, 12);
			long offset = ByteBuffer.ReadInt8(command, 20);
			int length = ByteBuffer.ReadInt4(command, 28);

			try {
				byte[] buf = dbInterface.GetStreamableObjectPart(resultId, streamableObjectId, offset, length);

				MemoryStream output = new MemoryStream();
				BinaryWriter writer = new BinaryWriter(output, Encoding.Unicode);

				writer.Write(dispatchId);
				writer.Write(ProtocolConstants.Success);

				writer.Write(buf.Length);
				writer.Write(buf, 0, buf.Length);
				writer.Flush();
				return output.ToArray();
			} catch (DataException e) {
				return Exception(dispatchId, e);
			}

		}

		/// <summary>
		/// Disposes of a streamable object.
		/// </summary>
		/// <param name="dispatchId"></param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] DisposeStreamableObject(int dispatchId, byte[] command) {
			int resultId = ByteBuffer.ReadInt4(command, 8);
			long streamableObjectId = ByteBuffer.ReadInt8(command, 12);

			try {
				// Pass this through to the underlying database interface.
				dbInterface.DisposeStreamableObject(resultId, streamableObjectId);

				// Return operation success.
				return SimpleSuccess(dispatchId);
			} catch (DataException e) {
				return Exception(dispatchId, e);
			}
		}

		/// <summary>
		/// Disposes of a result set we queries via the <see cref="ProtocolConstants.Query"/> Query.
		/// </summary>
		/// <param name="dispatchId"></param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] DisposeResult(int dispatchId, byte[] command) {
			// Get the result id.
			int resultId = ByteBuffer.ReadInt4(command, 8);

			try {
				// Dispose the table.
				dbInterface.DisposeResult(resultId);
				// Return operation success.
				return SimpleSuccess(dispatchId);
			} catch (DataException e) {
				return Exception(dispatchId, e);
			}
		}


		// ---------- Abstract methods ----------

		/// <summary>
		/// Sends an event to the client.
		/// </summary>
		/// <param name="eventMsg"></param>
		/// <remarks>
		/// This is used to notify the client of trigger events, etc.
		/// <para>
		/// <b>Security Issue</b>: This is always invoked by the <see cref="DatabaseDispatcher"/>. 
		/// We have to be careful that this method isn't allowed to block. Otherwise the 
		/// <see cref="DatabaseDispatcher"/> thread will be out of operation. Unfortunately assuring 
		/// this may not be possible until non-blocking IO, or we use datagrams for transmission. I 
		/// know for sure that the TCP implementation is vunrable. If the client doesn't 'read' what 
		/// we are sending then this'll block when the buffers become full.
		/// </para>
		/// </remarks>
		protected abstract void SendEvent(byte[] eventMsg);

		/// <summary>
		/// Closes the connection with the client.
		/// </summary>
		public abstract void Close();

		/// <summary>
		/// Returns true if the connection to the client is closed.
		/// </summary>
		public abstract bool IsClosed { get; }

		public Database Database {
			get { return database; }
		}

		// ---------- Finalize ----------
		~Processor() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}