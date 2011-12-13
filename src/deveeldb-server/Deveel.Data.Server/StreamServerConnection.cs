using System;
using System.IO;
using System.Text;

using Deveel.Data.Control;
using Deveel.Data.Protocol;

namespace Deveel.Data.Server {
	/// <summary>
	/// A generic stream protocol server that reads _queries from a stream from 
	/// each connection and dispatches the _queries appropriately.
	/// </summary>
	public abstract class StreamServerConnection : Processor, IServerConnection {
		/// <summary>
		/// The size in bytes of the buffer used for writing information onto the
		/// output stream to the client.
		/// </summary>
		private const int OutputBufferSize = 32768;

		/// <summary>
		/// The size in bytes of the buffer used for reading information from the
		/// input stream from the client.
		/// </summary>
		private const int InputBufferSize = 16384;

		/// <summary>
		/// The <see cref="LengthMarkedBufferedInputStream"/> we use to poll for _queries 
		/// from the client.
		/// </summary>
		private readonly LengthMarkedBufferedInputStream markedInput;

		/// <summary>
		/// The output stream to the client formatted as a <see cref="BinaryWriter"/>.
		/// </summary>
		private readonly BinaryWriter output;

		/// <summary>
		/// Sets up the protocol connection.
		/// </summary>
		/// <param name="hostString"></param>
		/// <param name="input"></param>
		/// <param name="output"></param>
		/// <param name="controller"></param>
		protected StreamServerConnection(DbController controller, string hostString, Stream input, Stream output)
			: base(controller, hostString) {
			if (!typeof(IInputStream).IsInstanceOfType(input))
				throw new ArgumentException("The input given must inherits IInputStream.", "input");

			markedInput = new LengthMarkedBufferedInputStream(input as IInputStream);
			this.output = new BinaryWriter(new BufferedStream(output, OutputBufferSize), Encoding.Unicode);

		}

		// NOTE: There's a security issue for this method.  See Processor
		//   for the details.
		protected override void SendEvent(byte[] eventMsg) {
			lock (output) {
				// Query length...
				output.Write(4 + 4 + eventMsg.Length);
				// Dispatch id...
				output.Write(-1);
				// Query id...
				output.Write(ProtocolConstants.DatabaseEvent);
				// The message...
				output.Write(eventMsg, 0, eventMsg.Length);
				// Flush Query to server.
				output.Flush();
			}
		}

		// ---------- Implemented from IServerConnection ----------

		public bool RequestPending() {
			ClientConnectionState state = ClientState;
			int maxSize = 256;
			if (state == ClientConnectionState.Processing)
				 maxSize = Int32.MaxValue;

			return markedInput.PollForCommand(maxSize);
		}

		public void ProcessRequest() {
			// Only allow 8 _queries to execute in sequence before we free this
			// worker to the worker pool.
			// We have a limit incase of potential DOS problems.
			int sequenceLimit = 8;

			// Read the Query into a 'byte[]' array and pass to the Query
			// processor.
			int commandLength = markedInput.Available;
			while (commandLength > 0) {
				byte[] command = new byte[commandLength];
				int readIndex = 0;
				while (readIndex < commandLength) {
					readIndex += markedInput.Read(command, readIndex, (commandLength - readIndex));
				}

				// Process the Query
				byte[] response = ProcessCommand(command);
				if (response != null) {
					lock (output) {
						// Write the response to the client.
						output.Write(response.Length);
						output.Write(response);
						output.Flush();
					}
				}

				// If there's another Query pending then process that one also,
				commandLength = 0;
				if (sequenceLimit > 0) {
					if (RequestPending()) {
						commandLength = markedInput.Available;
						--sequenceLimit;
					}
				}
			}
		}

		public void BlockForRequest() {
			markedInput.BlockForCommand();
		}

		public void Ping() {
			lock (output) {
				// Query length...
				output.Write(8);
				// Dispatch id...
				output.Write(-1);
				// Ping Query id...
				output.Write(ProtocolConstants.Ping);
				// Flush Query to server.
				output.Flush();
			}
		}
	}
}