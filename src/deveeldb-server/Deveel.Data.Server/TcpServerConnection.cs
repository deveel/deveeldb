using System;
using System.IO;
using System.Net.Sockets;

using Deveel.Data.Control;
using Deveel.Data.Util;

namespace Deveel.Data.Server {
	/// <summary>
	/// A <see cref="IServerConnection"/> that processes queries from a client 
	/// from a TCP/IP socket.
	/// </summary>
	sealed class TcpServerConnection : StreamServerConnection {
		/// <summary>
		/// The socket connection with the client.
		/// </summary>
		private readonly Socket connection;

		/// <summary>
		/// Is set to true when the connection to the client is closed.
		/// </summary>
		private bool is_closed = false;

		/// <summary>
		/// Constructs the <see cref="IServerConnection"/> object.
		/// </summary>
		/// <param name="controller"></param>
		/// <param name="socket"></param>
		internal TcpServerConnection(TcpServerController controller, string host_string, Socket socket)
			: base(controller, host_string, new NetworkInputStream(socket), new NetworkStream(socket, FileAccess.Write)) {
			this.connection = socket;
		}

		/// <summary>
		/// Completely closes the connection to the client.
		/// </summary>
		public override void Close() {
			try {
				// Dispose the processor
				Dispose();
			} catch (Exception e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
			}
			// Close the socket
			connection.Close();
			is_closed = true;
		}

		public override bool IsClosed {
			get { return is_closed; }
		}
	}
}