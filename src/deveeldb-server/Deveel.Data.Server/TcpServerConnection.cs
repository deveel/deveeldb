using System;
using System.IO;
using System.Net.Sockets;

using Deveel.Data.Control;
using Deveel.Data.Protocol;

namespace Deveel.Data.Server {
	/// <summary>
	/// A <see cref="IServerConnection"/> that processes queries from a client 
	/// from a TCP/IP socket.
	/// </summary>
	sealed class TcpServerConnection : StreamServerConnection {
		/// <summary>
		/// The socket connection with the client.
		/// </summary>
		private readonly Socket socket;

		/// <summary>
		/// Is set to true when the connection to the client is closed.
		/// </summary>
		private bool isClosed;

		/// <summary>
		/// Constructs the <see cref="IServerConnection"/> object.
		/// </summary>
		/// <param name="controller"></param>
		/// <param name="hostString"></param>
		/// <param name="socket"></param>
		internal TcpServerConnection(TcpServerController controller, string hostString, Socket socket)
			: base(controller.Controller, hostString, new NetworkInputStream(socket), new NetworkStream(socket, FileAccess.Write)) {
			this.socket = socket;
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
			try {
				// Close the socket
				socket.Close();
			} finally {
				isClosed = true;
			}
		}

		public override bool IsClosed {
			get { return isClosed; }
		}
	}
}