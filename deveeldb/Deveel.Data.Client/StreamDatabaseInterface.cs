//  
//  StreamDatabaseInterface.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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