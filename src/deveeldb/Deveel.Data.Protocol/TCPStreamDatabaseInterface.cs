// 
//  Copyright 2010-2014 Deveel
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

		internal TCPStreamDatabaseInterface(String host, int port, string initial_database)
			: base(initial_database) {
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