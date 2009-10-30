using System;
using System.Collections;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;

using Deveel.Data.Client;
using Deveel.Data.Control;
using Deveel.Data.Util;
using Deveel.Diagnostics;

namespace Deveel.Data.Server {
	/// <summary>
	/// This processes commands from a client and dispatches the commands 
	/// to the database.
	/// </summary>
	/// <remarks>
	/// This is a state based class. There is a single processor for each client 
	/// connected. This class is designed to be flexible enough to handle packet 
	/// based protocols as well as stream based protocols.
	/// </remarks>
	abstract class Processor {
		/// <summary>
		/// The version of the server protocol.
		/// </summary>
		private const int SERVER_VERSION = 1;

		/// <summary>
		/// The current state we are in.
		/// </summary>
		private ConnectionState state;

		/// <summary>
		/// Number of authentications tried.
		/// </summary>
		private int authentication_tries;

		private readonly string host_string;

		private readonly TcpServerController controller;
		private readonly Hashtable db_interfaces;
		private DatabaseInterface db_interface;

		private Database database;

		internal Processor(TcpServerController controller, string host_string) {
			this.host_string = host_string;
			db_interfaces = new Hashtable();
			this.controller = controller;
			state = 0;
			authentication_tries = 0;
			db_call_back = new DatabaseCallBackImpl(this);
		}

		/// <summary>
		/// The database call back method that sends database events back to the client.
		/// </summary>
		private readonly IDatabaseCallBack db_call_back;

		private class DatabaseCallBackImpl : IDatabaseCallBack {
			public DatabaseCallBackImpl(Processor processor) {
				this.processor = processor;
			}

			private readonly Processor processor;

			public void OnDatabaseEvent(int event_type, String event_message) {
				try {
					// Format the call back and send the event.
					MemoryStream bout = new MemoryStream();
					BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);
					dout.Write(event_type);
					dout.Write(event_message);
					processor.SendEvent(bout.ToArray());
				} catch (IOException e) {
					Debug.Write(DebugLevel.Error, this, "IO Error: " + e.Message);
					Debug.WriteException(e);
				}
			}
		}

		private bool ChangeDatabaseInterface(string databaseName) {
			if (!controller.DatabaseExists(databaseName))
				return false;

			DatabaseInterface dbi = db_interfaces[databaseName] as DatabaseInterface;
			if (dbi == null) {
				dbi = new DatabaseInterface(controller, databaseName, host_string);
				db_interfaces[databaseName] = dbi;
			}

			database = controller.GetDatabase(databaseName);
			db_interface = dbi;
			return true;
		}

		protected static void PrintByteArray(byte[] array) {
			Console.Out.WriteLine("Length: " + array.Length);
			for (int i = 0; i < array.Length; ++i) {
				Console.Out.Write(array[i]);
				Console.Out.Write(", ");
			}
		}

		/// <summary>
		/// Processes a single command from the client.
		/// </summary>
		/// <param name="command"></param>
		/// <returns>
		/// Returns a byte array and the response is written out as a byte array, or
		/// <b>null</b> if the connection has been closed.
		/// </returns>
		internal byte[] ProcessCommand(byte[] command) {

			//    PrintByteArray(command);

			if (state == ConnectionState.Closed) {
				// State 0 means we looking for the header...
				BinaryReader reader = new BinaryReader(new MemoryStream(command), Encoding.ASCII);
				/*
				int magic = ByteBuffer.ReadInt4(command, 0);
				// The driver version number
				int maj_ver = ByteBuffer.ReadInt4(command, 4);
				int min_ver = ByteBuffer.ReadInt4(command, 8);
				*/
				reader.ReadInt32();		// magic
				reader.ReadInt32();		// server major version
				reader.ReadInt32();		// server minor version

				string databaseName = reader.ReadString();

				if (!ChangeDatabaseInterface(databaseName))
					return Single(ProtocolConstants.DATABASE_NOT_FOUND);

				Version version = Assembly.GetExecutingAssembly().GetName().Version;

				byte[] ack_command = new byte[4 + 1 + 4 + 4 + 1];
				// Send back an acknowledgement and the version number of the server
				ByteBuffer.WriteInteger(ProtocolConstants.ACKNOWLEDGEMENT, ack_command, 0);
				ack_command[4] = 1;
				ByteBuffer.WriteInteger(version.Major, ack_command, 5);
				ByteBuffer.WriteInteger(version.Minor, ack_command, 9);
				ack_command[13] = 0;

				// Set to the next state.
				state = ConnectionState.NotAuthenticated;

				// Return the acknowledgement
				return ack_command;

				//      // We accept drivers equal or less than 1.00 currently.
				//      if ((maj_ver == 1 && min_ver == 0) || maj_ver == 0) {
				//        // Go to next state.
				//        state = 4;
				//        return Single(ACKNOWLEDGEMENT);
				//      }
				//      else {
				//        // Close the connection if driver invalid.
				//        Close();
				//      }
				//
				//      return null;
			} else if (state == ConnectionState.NotAuthenticated) {
				// State 4 means we looking for username and password...
				MemoryStream bin = new MemoryStream(command);
				BinaryReader din = new BinaryReader(bin, Encoding.ASCII);
				string default_schema = din.ReadString();
				string username = din.ReadString();
				string password = din.ReadString();

				try {
					bool good = db_interface.Login(default_schema, username, password, db_call_back);
					if (good == false) {
						// Close after 12 tries.
						if (authentication_tries >= 12) {
							Close();
						} else {
							++authentication_tries;
							return Single(ProtocolConstants.USER_AUTHENTICATION_FAILED);
						}
					} else {
						state = ConnectionState.Processing;
						return Single(ProtocolConstants.USER_AUTHENTICATION_PASSED);
					}
				} catch (DataException e) {

				}

				return null;
			} else if (state == ConnectionState.Processing) {
				// Process the query
				return ProcessQuery(command);
			} else {
				throw new Exception("Illegal state: " + state);
			}

		}

		/// <summary>
		/// Returns the state of the connection.
		/// </summary>
		/// <remarks>
		/// 0 = not logged in yet.  1 = logged in.
		/// </remarks>
		internal ConnectionState State {
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
		/// <param name="dispatch_id"></param>
		/// <param name="e"></param>
		/// <returns></returns>
		private static byte[] Exception(int dispatch_id, DataException e) {
			int code = /* TODO: e.ErrorCode */ -1;
			String msg = e.Message;
			if (msg == null) {
				msg = "NULL exception message";
			}
			String server_msg = "";
			String stack_trace = "";

			if (e is DbDataException) {
				DbDataException me = (DbDataException)e;
				server_msg = me.ServerErrorMessage;
				stack_trace = me.ServerErrorStackTrace;
			} else {
				stack_trace = e.StackTrace;
			}

			MemoryStream bout = new MemoryStream();
			BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);
			dout.Write(dispatch_id);
			dout.Write(ProtocolConstants.EXCEPTION);
			dout.Write(code);
			dout.Write(msg);
			dout.Write(stack_trace);

			return bout.ToArray();
		}

		/// <summary>
		/// Creates a response that indicates a simple success of an operation 
		/// with the given dispatch id.
		/// </summary>
		/// <param name="dispatch_id"></param>
		/// <returns></returns>
		private static byte[] SimpleSuccess(int dispatch_id) {
			byte[] buf = new byte[8];
			ByteBuffer.WriteInteger(dispatch_id, buf, 0);
			ByteBuffer.WriteInteger(ProtocolConstants.SUCCESS, buf, 4);
			return buf;
		}

		/// <summary>
		/// Processes a query on the byte[] array and returns the result.
		/// </summary>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] ProcessQuery(byte[] command) {
			byte[] result;

			// The first int is the command.
			int ins = ByteBuffer.ReadInt4(command, 0);

			// Otherwise must be a dispatch type request.
			// The second is the dispatch id.
			int dispatch_id = ByteBuffer.ReadInt4(command, 4);

			if (dispatch_id == -1) {
				throw new Exception("Special case dispatch id of -1 in query");
			}

			if (ins == ProtocolConstants.CHANGE_DATABASE) {
				result = ChangeDatabase(dispatch_id, command);
			} else if (ins == ProtocolConstants.RESULT_SECTION) {
				result = ResultSection(dispatch_id, command);
			} else if (ins == ProtocolConstants.QUERY) {
				result = QueryCommand(dispatch_id, command);
			} else if (ins == ProtocolConstants.PUSH_STREAMABLE_OBJECT_PART) {
				result = PushStreamableObjectPart(dispatch_id, command);
			} else if (ins == ProtocolConstants.DISPOSE_RESULT) {
				result = DisposeResult(dispatch_id, command);
			} else if (ins == ProtocolConstants.STREAMABLE_OBJECT_SECTION) {
				result = StreamableObjectSection(dispatch_id, command);
			} else if (ins == ProtocolConstants.DISPOSE_STREAMABLE_OBJECT) {
				result = DisposeStreamableObject(dispatch_id, command);
			} else if (ins == ProtocolConstants.CLOSE) {
				Close();
				result = null;
			} else {
				throw new Exception("Command (" + ins + ") not understood.");
			}

			return result;

		}

		/// <summary>
		///  Disposes of this processor.
		/// </summary>
		internal void Dispose() {
			try {
				if (db_interfaces.Count > 0) {
					foreach (DatabaseInterface databaseInterface in db_interfaces.Values) {
						(databaseInterface as IDisposable).Dispose();
					}
				}
				db_interfaces.Clear();
			} catch (Exception e) {
				Debug.WriteException(DebugLevel.Error, e);
			}
		}


		// ---------- Primitive commands ----------

		private byte [] ChangeDatabase(int dispatch_id, byte[] command) {
			// Read the query from the command.
			MemoryStream bin = new MemoryStream(command, 8, command.Length - 8);
			BinaryReader din = new BinaryReader(bin, Encoding.Unicode);

			string databaseName = din.ReadString();

			try {
				db_interface.ChangeDatabase(databaseName);
				state = ConnectionState.NotAuthenticated;
				return SimpleSuccess(dispatch_id);
			} catch (DataException e) {
				return Exception(dispatch_id, e);
			}
		}

		/// <summary>
		/// Executes a query and returns the header for the result in the response.
		/// </summary>
		/// <param name="dispatch_id">The number we need to respond with.</param>
		/// <param name="command"></param>
		/// <remarks>
		/// This keeps track of all result sets because sections of the result are 
		/// later queries via the <see cref="ProtocolConstants.RESULT_SECTION"/>
		/// command.
		/// </remarks>
		/// <returns></returns>
		private byte[] QueryCommand(int dispatch_id, byte[] command) {

			// Read the query from the command.
			MemoryStream bin = new MemoryStream(command, 8, command.Length - 8);
			BinaryReader din = new BinaryReader(bin, Encoding.Unicode);
			SqlCommand query = SqlCommand.ReadFrom(din);

			try {
				// Do the query
				IQueryResponse response = db_interface.ExecuteQuery(query);

				// Prepare the stream to output the response to,
				MemoryStream bout = new MemoryStream();
				BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);

				dout.Write(dispatch_id);
				dout.Write(ProtocolConstants.SUCCESS);

				// The response sends the result id, the time the query took, the
				// total row count, and description of each column in the result.
				dout.Write(response.ResultId);
				dout.Write(response.QueryTimeMillis);
				dout.Write(response.RowCount);
				int col_count = response.ColumnCount;
				dout.Write(col_count);
				for (int i = 0; i < col_count; ++i) {
					response.GetColumnDescription(i).WriteTo(dout);
				}

				return bout.ToArray();

			} catch (DataException e) {
				//      debug.writeException(e);
				return Exception(dispatch_id, e);
			}

		}


		/// <summary>
		/// Pushes a part of a streamable object onto the server.
		/// </summary>
		/// <param name="dispatch_id">The number we need to respond with.</param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] PushStreamableObjectPart(int dispatch_id, byte[] command) {
			ReferenceType type = (ReferenceType) command[8];
			long object_id = ByteBuffer.ReadInt8(command, 9);
			long object_length = ByteBuffer.ReadInt8(command, 17);
			int length = ByteBuffer.ReadInt4(command, 25);
			byte[] ob_buf = new byte[length];
			Array.Copy(command, 29, ob_buf, 0, length);
			long offset = ByteBuffer.ReadInt8(command, 29 + length);

			try {
				// Pass this through to the underlying database interface.
				db_interface.PushStreamableObjectPart(type, object_id, object_length, ob_buf, offset, length);

				// Return operation success.
				return SimpleSuccess(dispatch_id);
			} catch (DataException e) {
				return Exception(dispatch_id, e);
			}
		}


		/// <summary>
		/// Responds with a part of the result set of a query made via the 
		/// <see cref="ProtocolConstants.QUERY"/> command.
		/// </summary>
		/// <param name="dispatch_id">The number we need to respond with.</param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] ResultSection(int dispatch_id, byte[] command) {
			int result_id = ByteBuffer.ReadInt4(command, 8);
			int row_number = ByteBuffer.ReadInt4(command, 12);
			int row_count = ByteBuffer.ReadInt4(command, 16);

			try {
				// Get the result part...
				ResultPart block = db_interface.GetResultPart(result_id, row_number, row_count);

				MemoryStream bout = new MemoryStream();
				BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);

				dout.Write(dispatch_id);
				dout.Write(ProtocolConstants.SUCCESS);

				// Send the contents of the result set.
				// HACK - Work out column count by dividing number of entries in block
				//   by number of rows.
				int col_count = block.Count / row_count;
				dout.Write(col_count);
				int bsize = block.Count;
				for (int index = 0; index < bsize; ++index) {
					ObjectTransfer.WriteTo(dout, block[index]);
				}

				return bout.ToArray();
			} catch (DataException e) {
				return Exception(dispatch_id, e);
			}
		}

		/// <summary>
		/// Returns a section of a streamable object.
		/// </summary>
		/// <param name="dispatch_id">The number we need to respond with.</param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] StreamableObjectSection(int dispatch_id, byte[] command) {
			int result_id = ByteBuffer.ReadInt4(command, 8);
			long streamable_object_id = ByteBuffer.ReadInt8(command, 12);
			long offset = ByteBuffer.ReadInt8(command, 20);
			int length = ByteBuffer.ReadInt4(command, 28);

			try {
				StreamableObjectPart ob_part = db_interface.GetStreamableObjectPart(result_id, streamable_object_id, offset, length);

				MemoryStream bout = new MemoryStream();
				BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);

				dout.Write(dispatch_id);
				dout.Write(ProtocolConstants.SUCCESS);

				byte[] buf = ob_part.Contents;
				dout.Write(buf.Length);
				dout.Write(buf, 0, buf.Length);

				return bout.ToArray();
			} catch (DataException e) {
				return Exception(dispatch_id, e);
			}

		}

		/// <summary>
		/// Disposes of a streamable object.
		/// </summary>
		/// <param name="dispatch_id"></param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] DisposeStreamableObject(int dispatch_id, byte[] command) {
			int result_id = ByteBuffer.ReadInt4(command, 8);
			long streamable_object_id = ByteBuffer.ReadInt8(command, 12);

			try {
				// Pass this through to the underlying database interface.
				db_interface.DisposeStreamableObject(result_id, streamable_object_id);

				// Return operation success.
				return SimpleSuccess(dispatch_id);

			} catch (DataException e) {
				return Exception(dispatch_id, e);
			}
		}

		/// <summary>
		/// Disposes of a result set we queries via the <see cref="ProtocolConstants.QUERY"/> command.
		/// </summary>
		/// <param name="dispatch_id"></param>
		/// <param name="command"></param>
		/// <returns></returns>
		private byte[] DisposeResult(int dispatch_id, byte[] command) {

			// Get the result id.
			int result_id = ByteBuffer.ReadInt4(command, 8);

			try {
				// Dispose the table.
				db_interface.DisposeResult(result_id);
				// Return operation success.
				return SimpleSuccess(dispatch_id);
			} catch (DataException e) {
				return Exception(dispatch_id, e);
			}
		}






		// ---------- Abstract methods ----------

		/// <summary>
		/// Sends an event to the client.
		/// </summary>
		/// <param name="event_msg"></param>
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
		protected abstract void SendEvent(byte[] event_msg);

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
			try {
				Dispose();
			} catch (Exception e) { /* ignore */ }
		}
	}
}