using System;
using System.IO;
using System.Text;

using Deveel.Data.Client;
using Deveel.Data.Control;
using Deveel.Data.Util;

namespace Deveel.Data.Server {
	/// <summary>
	/// A generic stream protocol server that reads commands from a stream from 
	/// each connection and dispatches the commands appropriately.
	/// </summary>
	internal abstract class StreamServerConnection : Processor, IServerConnection {
		/// <summary>
		/// The size in bytes of the buffer used for writing information onto the
		/// output stream to the client.
		/// </summary>
		private const int OUTPUT_BUFFER_SIZE = 32768;

		/// <summary>
		/// The size in bytes of the buffer used for reading information from the
		/// input stream from the client.
		/// </summary>
		private const int INPUT_BUFFER_SIZE = 16384;

		/// <summary>
		/// The <see cref="LengthMarkedBufferedInputStream"/> we use to poll for commands 
		/// from the client.
		/// </summary>
		private LengthMarkedBufferedInputStream marked_input;

		/// <summary>
		/// The output stream to the client formatted as a <see cref="BinaryWriter"/>.
		/// </summary>
		private BinaryWriter output;

		/// <summary>
		/// Sets up the protocol connection.
		/// </summary>
		/// <param name="db_interface"></param>
		/// <param name="input"></param>
		/// <param name="output"></param>
		internal StreamServerConnection(TcpServerController controller, string host_string, Stream input, Stream output)
			: base(controller, host_string) {

			this.marked_input = new LengthMarkedBufferedInputStream(input as IInputStream);
			this.output = new BinaryWriter(new BufferedStream(output, OUTPUT_BUFFER_SIZE), Encoding.Unicode);

		}

		// ---------- Implemented from JDBCConnection ----------

		// NOTE: There's a security issue for this method.  See Processor
		//   for the details.
		protected override void SendEvent(byte[] event_msg) {
			lock (output) {
				// Command length...
				output.Write(4 + 4 + event_msg.Length);
				// Dispatch id...
				output.Write(-1);
				// Command id...
				output.Write(ProtocolConstants.DATABASE_EVENT);
				// The message...
				output.Write(event_msg, 0, event_msg.Length);
				// Flush command to server.
				output.Flush();
			}
		}

		// ---------- Implemented from IServerConnection ----------

		public bool RequestPending() {
			ConnectionState state = State;
			if (state == ConnectionState.Processing) {
				return marked_input.PollForCommand(Int32.MaxValue);
			} else {
				return marked_input.PollForCommand(256);
			}
		}

		public void ProcessRequest() {
			// Only allow 8 commands to execute in sequence before we free this
			// worker to the worker pool.
			// We have a limit incase of potential DOS problems.
			int sequence_limit = 8;

			// Read the command into a 'byte[]' array and pass to the command
			// processor.
			int com_length = marked_input.Available;
			while (com_length > 0) {
				byte[] command = new byte[com_length];
				int read_index = 0;
				while (read_index < com_length) {
					read_index += marked_input.Read(command, read_index, (com_length - read_index));
				}

				// Process the command
				byte[] response = ProcessCommand(command);
				if (response != null) {

					lock (output) {
						// Write the response to the client.
						output.Write(response.Length);
						output.Write(response);
						output.Flush();
					}

				}

				// If there's another command pending then process that one also,
				com_length = 0;
				if (sequence_limit > 0) {
					if (RequestPending()) {
						com_length = marked_input.Available;
						--sequence_limit;
					}
				}

			} // while (com_length > 0)

			//    // Response...
			//    PrintByteArray(response);
		}

		public void BlockForRequest() {
			marked_input.blockForCommand();
		}

		public void Ping() {
			lock (output) {
				// Command length...
				output.Write(8);
				// Dispatch id...
				output.Write(-1);
				// Ping command id...
				output.Write(ProtocolConstants.PING);
				// Flush command to server.
				output.Flush();
			}
		}
	}
}