//  
//  TCPStreamDatabaseInterface.cs
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
using System.Data;
using System.IO;
using System.Net.Sockets;

namespace Deveel.Data.Client {
	/// <summary>
	/// Connection to the database via the TCP protocol.
	/// </summary>
	class TCPStreamDatabaseInterface : StreamDatabaseInterface {
		/// <summary>
		/// The name of the host we are connected to.
		/// </summary>
		private readonly String host;

		/// <summary>
		/// The port we are connected to.
		/// </summary>
		private readonly int port;

		/// <summary>
		/// The Socket connection.
		/// </summary>
		private Socket socket;

		internal TCPStreamDatabaseInterface(String host, int port) {
			this.host = host;
			this.port = port;
		}

		/// <summary>
		/// Connects to the database.
		/// </summary>
		internal void ConnectToDatabase() {
			if (socket != null) {
				throw new DataException("Connection already established.");
			}
			try {
				// Open a socket connection to the server.
				socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				socket.Connect(host, port);
				// Setup the stream with the given input and output streams.
				Setup(new NetworkStream(socket, FileAccess.Read), new NetworkStream(socket, FileAccess.Write));
			} catch (IOException e) {
				throw new DataException(e.Message);
			}
		}
	}
}