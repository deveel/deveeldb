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
			//    Console.Out.WriteLine("rawin: " + rawin);
			//    Console.Out.WriteLine("rawout: " + rawout);
			if (rawin == null || rawout == null) {
				throw new IOException("rawin or rawin is null");
			}
			// Get the input and output and wrap around Data streams.
			input = new BinaryReader(new BufferedStream(rawin, 32768));
			output = new BinaryWriter(new BufferedStream(rawout, 32768));
		}

		/// <inheritdoc/>
		internal override void WriteCommandToServer(byte[] command, int offset, int size) {
			output.Write(size);
			output.Write(command, 0, size);
			output.Flush();
		}

		/// <inheritdoc/>
		internal override byte[] NextCommandFromServer(int timeout) {
			if (closed) {
				throw new IOException("IDatabaseInterface is closed!");
			}
			try {
				//      Console.Out.WriteLine("I'm waiting for a command: " + this);
				//      new Error().printStackTrace();
				int command_length = input.ReadInt32();
				byte[] buf = new byte[command_length];
				input.Read(buf, 0, command_length);
				return buf;
			} catch (NullReferenceException) {
				Console.Out.WriteLine("Throwable generated at: " + this);
				throw;
			}
		}

		/// <inheritdoc/>
		internal override void CloseConnection() {
			//    Console.Out.WriteLine("Closed: " + this);
			closed = true;
			try {
				output.Close();
			} catch (IOException) {
				input.Close();
				throw;
			}
			input.Close();
		}
	}
}