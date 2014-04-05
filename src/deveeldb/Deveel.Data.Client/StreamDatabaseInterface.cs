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
using System.IO;

namespace Deveel.Data.Client {
	/// <summary>
	/// An stream implementation of an interface to a database.
	/// </summary>
	/// <remarks>
	/// This is a stream based communication protocol.
	/// </remarks>
	class StreamDatabaseInterface : RemoteDatabaseInterface {
		internal StreamDatabaseInterface(string initial_database)
			: base(initial_database) {
		}

		/// <summary>
		/// The data output stream for the db protocol.
		/// </summary>
		protected BinaryWriter output;
		/// <summary>
		/// The data input stream for the db protocol.
		/// </summary>
		protected BinaryReader input;

		private bool closed = false;

		/// <summary>
		/// Sets up the stream connection with the given input/output stream.
		/// </summary>
		/// <param name="rawin"></param>
		/// <param name="rawout"></param>
		internal void Setup(Stream rawin, Stream rawout) {
			if (rawin == null || rawout == null) {
				throw new IOException("rawin or rawin is null");
			}
			// Get the input and output and wrap around Data streams.
			input = new BinaryReader(new BufferedStream(rawin, 32768));
			output = new BinaryWriter(new BufferedStream(rawout, 32768));
		}

		/// <inheritdoc/>
		protected override void SendCommand(byte[] command, int offset, int size) {
			output.Write(size);
			output.Write(command, 0, size);
			output.Flush();
		}

		/// <inheritdoc/>
		protected override byte[] ReceiveCommand(int timeout) {
			if (closed) {
				throw new IOException("IDatabaseInterface is closed!");
			}
			try {
				int commandLength = input.ReadInt32();
				byte[] buf = new byte[commandLength];
				input.Read(buf, 0, commandLength);
				return buf;
			} catch (NullReferenceException) {
				Console.Out.WriteLine("Throwable generated at: " + this);
				throw;
			}
		}

		/// <inheritdoc/>
		protected override void CloseConnection() {
			try {
				if (output != null)
					output.Close();
			} finally {
				if (input != null)
					input.Close();

				closed = true;
			}
		}
	}
}