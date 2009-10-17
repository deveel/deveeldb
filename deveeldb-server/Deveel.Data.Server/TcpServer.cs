// 
//  TcpServer.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

using Deveel.Data.Control;
using Deveel.Diagnostics;

namespace Deveel.Data.Server {
	/// <summary>
	/// A TCP/IP socket server that opens a single port and allows clients 
	/// to connect through the port to talk with the database. 
	/// </summary>
	public class TcpServer {
		/// <summary>
		/// The <see cref="TcpServerController"/> that handles this instance of
		/// the server, and provides information concerning the databases served.
		/// </summary>
		private TcpServerController server_controller;

		/// <summary>
		/// The <see cref="IConnectionPoolServer"/> that polls the <see cref="IServerConnection"/> 
		/// for new commands to process.
		/// </summary>
		private IConnectionPoolServer connection_pool;

		/// <summary>
		/// The <see cref="Socket"/> object where the database server is bound.
		/// </summary>
		private Socket server_socket;

		/// <summary>
		/// The <see cref="IPAddress"/> the server is bound to.
		/// </summary>
		private IPAddress address;

		/// <summary>
		/// The port the server is lestining on.
		/// </summary>
		private int port;

		/// <summary>
		/// The connection pool model used for this server.
		/// </summary>
		private ConnectionPoolModel connection_pool_model;

		/// <summary>
		/// Constructs the <see cref="TcpServer"/> over the given 
		/// <see cref="DatabaseSystem"/> configuration.
		/// </summary>
		/// <param name="server_controller"></param>
		public TcpServer(TcpServerController server_controller) {
			this.server_controller = server_controller;
		}

		/// <summary>
		/// Returns the port the server is on.
		/// </summary>
		public int Port {
			get { return port; }
		}

		/// <summary>
		/// Checks to see if there's already something listening on the port.
		/// </summary>
		/// <param name="bind_address"></param>
		/// <param name="tcp_port"></param>
		/// <returns>
		/// Returns true if the port in the configuration is available, otherwise returns false.
		/// </returns>
		public bool CheckAvailable(IPAddress bind_address, int tcp_port) {
			// MAJOR MAJOR HACK: We attempt to bind to the JDBC Port and if we get
			//   an error then most likely there's already a database running on this
			//   host.

			try {
				// Bind the ServerSocket objects to the ports.
				server_socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				server_socket.Bind(new IPEndPoint(bind_address, tcp_port));
				server_socket.Listen(50);
				server_socket.Close();
			} catch (IOException e) {
				// If error then return false.
				return false;
			}

			return true;
		}

		/// <summary>
		/// Starts the server running.
		/// </summary>
		/// <param name="bind_address"></param>
		/// <param name="tcp_port"></param>
		/// <param name="model"></param>
		/// <remarks>
		/// This method returns immediately but spawns its own thread.
		/// </remarks>
		public void Start(IPAddress bind_address, int tcp_port, ConnectionPoolModel model) {
			this.address = bind_address;
			this.port = tcp_port;
			this.connection_pool_model = model;

			// Choose our connection pool implementation
			if (connection_pool_model == ConnectionPoolModel.MultiThreaded) {
				this.connection_pool = new MultiThreadedConnectionPoolServer(server_controller);
			} else if (connection_pool_model == ConnectionPoolModel.SingleThreaded) {
				this.connection_pool = new SingleThreadedConnectionPoolServer(server_controller);
			}

			try {
				// Bind the ServerSocket object to the port.
				server_socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				server_socket.Bind(new IPEndPoint(bind_address, port));
				server_socket.Listen(50);
				//CHECK: SO_TIMEOUT
				server_socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, 0);
			} catch (IOException e) {
				Debug.WriteException(e);
				Debug.Write(DebugLevel.Error, this, "Unable to start a server socket on port: " + port);
				throw new Exception(e.Message);
			}

			// This thread blocks on the server socket.
			Thread listen_thread = new Thread(new ThreadStart(Listen));

			//    listen_thread.IsBackground = true;
			listen_thread.Name = "TCP/IP Socket Accept";

			listen_thread.Start();

		}

		private void Listen() {
			try {
				// Accept new connections and notify when one arrives
				while (true) {
					Socket s = server_socket.Accept();
					PortConnection(s);
				}
			} catch (IOException e) {
				Debug.WriteException(DebugLevel.Warning, e);
				Debug.Write(DebugLevel.Warning, this, "Socket listen thread died.");
			}
		}

		/// <summary>
		/// Called whenever a new connection has been received on the port.
		/// </summary>
		/// <param name="socket"></param>
		private void PortConnection(Socket socket) {
			// TCP connections are formatted as;
			IPEndPoint remoteEndPoint = (IPEndPoint) socket.RemoteEndPoint;
			IPEndPoint localEndPoint = (IPEndPoint) socket.LocalEndPoint;
			// 'TCP/[ip address]:[remote port]:[local port]'
			String host_string = "TCP/" + remoteEndPoint.Address + ":" + remoteEndPoint.Port + "@" +
								 localEndPoint.Address + ":" + localEndPoint.Port;
			//    String host_string =
			//      "Host [" + socket.getInetAddress().getHostAddress() + "] " +
			//      "port=" + socket.getPort() + " localport=" + socket.getLocalPort();
			// Make a new DatabaseInterface for this connection,
			TcpServerConnection connection = new TcpServerConnection(server_controller, host_string, socket);
			// Add the provider onto the queue of providers that are serviced by
			// the server.
			connection_pool.AddConnection(connection);
		}

		/// <summary>
		/// Closes the server.
		/// </summary>
		public void Close() {
			if (server_socket != null) {
				try {
					server_socket.Close();
				} catch (IOException e) {
					Debug.Write(DebugLevel.Error, this, "Error closing JDBC Server.");
					Debug.WriteException(e);
				}
			}
			connection_pool.Close();
		}

		/// <summary>
		/// Returns human understandable information about the server.
		/// </summary>
		/// <returns></returns>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("TCP Server (");
			buf.Append(connection_pool_model);
			buf.Append(") on ");
			if (address != null) {
				buf.Append(address.ToString());
				buf.Append(" ");
			}
			buf.Append("port: ");
			buf.Append(Port);
			return buf.ToString();
		}
	}
}