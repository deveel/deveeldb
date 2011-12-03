using System;
using System.Collections;
using System.IO;
using System.Threading;

using Deveel.Data.Control;
using Deveel.Diagnostics;

namespace Deveel.Data.Server {
	/// <summary>
	/// A generic database server class that provides a thread that dispatches 
	/// commands to the underlying database.
	/// </summary>
	/// <remarks>
	/// This class only provides a framework for creating a server. It doesn't 
	/// provide any implementation specifics for protocols.
	/// <para>
	/// An TCP implementation of this class would wait for connections and then 
	/// create a <see cref="IServerConnection"/> implementation and feed it into 
	/// the pool for processing. This object will then poll the <see cref="IServerConnection"/>
	/// until a command is pending, and then dispatch the command to a database 
	/// worker thread.
	/// </para>
	/// <para>
	/// This object will ping the clients every so often to see if they are alive.
	/// </para>
	/// </remarks>
	sealed class SingleThreadedConnectionPoolServer : IConnectionPoolServer {
		// NOTE: Should this be a configurable variable in the '.conf' file?
		/// <summary>
		/// The number of milliseconds between client pings.
		/// </summary>
		private static readonly int PING_BREAK = 45 * 1000;  //4 * 60 * 1000; (45 seconds)

		/// <summary>
		/// If this is set to true then the server periodically outputs statistics
		/// about the connections.
		/// </summary>
		private const bool DISPLAY_STATS = false;

		private TcpServerController controller;

		/// <summary>
		/// The list of <see cref="IServerConnection"/> objects that are pending to 
		/// be added into the current service provider list next time it is checked.
		/// </summary>
		private ArrayList pending_connections_list;

		/// <summary>
		/// The <see cref="ServerFarmer"/> object that polls for information from the 
		/// clients and dispatches the request to the worker threads.
		/// </summary>
		private ServerFarmer farmer;


		internal SingleThreadedConnectionPoolServer(TcpServerController controller) {
			this.controller = controller;
			pending_connections_list = new ArrayList();
			// Create the farmer thread that services all the connections.
			farmer = new ServerFarmer(this);
			farmer.Start();
		}

		/// <summary>
		/// Connects a new <see cref="IServerConnection"/> into the pool of connections 
		/// to clients that this server maintains.
		/// </summary>
		/// <param name="connection"></param>
		/// <remarks>
		/// We then cycle through these connections determining whether any commands are 
		/// pending. If a command is pending we spawn off a worker thread to do the task.
		/// </remarks>
		public void AddConnection(IServerConnection connection) {
			lock (pending_connections_list) {
				pending_connections_list.Add(connection);
			}
		}

		/// <summary>
		/// Closes this connection pool server down.
		/// </summary>
		public void Close() {
			farmer.Close();
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// This thread is a low priority thread that checks all the current service
		/// providers periodically to determine if there's any commands pending.
		/// </summary>
		private class ServerFarmer {
			private SingleThreadedConnectionPoolServer parent;
			private Thread thread;
			/// <summary>
			/// The list of <see cref="IServerConnection"/> objects that are currently being 
			/// serviced by this server.
			/// </summary>
			private ArrayList server_connections_list;

			// Staticial information collected.
			private int stat_display = 0;
			private int commands_run = 0;
			private int commands_waited = 0;

			/// <summary>
			/// If this is set to true, then the farmer run method should Close off.
			/// </summary>
			private bool farmer_closed;

			/// <summary>
			/// The number of milliseconds to wait between each poll of the 'available'
			/// method of the socket.
			/// </summary>
			/// <remarks>
			/// This value is determined by the configuration file during initialization.
			/// </remarks>
			private int poll_wait_time;



			/**
			 * The Constructor.
			 */
			public ServerFarmer(SingleThreadedConnectionPoolServer parent) {
				this.parent = parent;
				thread = new Thread(new ThreadStart(Run));
				//      setPriority(NORM_PRIORITY - 1);

				// The time in ms between each poll of the 'available' method.
				// Default is '3 ms'
				poll_wait_time = 3;

				server_connections_list = new ArrayList();
				farmer_closed = false;
			}

			public void Start() {
				thread.Start();
			}

			/// <summary>
			/// Establishes a connection with any current pending connections in the 
			/// <see cref="pending_connections_list"/>.
			/// </summary>
			private void EstablishPendingConnections() {
				lock (parent.pending_connections_list) {
					int len = parent.pending_connections_list.Count;
					// Move all pending connections into the current connections list.
					for (int i = 0; i < len; ++i) {
						// Get the connection and create the new connection state
						IServerConnection connection = (IServerConnection) parent.pending_connections_list[0];
							parent.pending_connections_list.RemoveAt(0);
						server_connections_list.Add(new ServerConnectionState(connection));
					}
				}
			}

			/// <summary>
			/// Checks each connection in the <see cref="server_connections_list"/> list.
			/// </summary>
			/// <remarks>
			/// If there is a command pending, and any previous commands on this connection 
			/// have completed, then this will spawn off a new process to deal with the command.
			/// </remarks>
			private void CheckCurrentConnections() {
				int len = server_connections_list.Count;
				for (int i = len - 1; i >= 0; --i) {
					ServerConnectionState connection_state =
								   (ServerConnectionState)server_connections_list[i];
					try {
						// Is this connection not currently processing a command?
						if (!connection_state.IsProcessingRequest) {
							IServerConnection connection = connection_state.Connection;
							// Does this connection have a request pending?
							if (connection_state.HasPendingCommand ||
								connection.RequestPending()) {
								// Set that we have a pending command
								connection_state.SetPendingCommand();
								connection_state.SetProcessingRequest();

								ServerConnectionState current_state = connection_state;
								ProcessRequestEventImpl requestCallback = new ProcessRequestEventImpl(this, current_state);
								connection.Database.Execute(null, null, requestCallback.Execute);

							} // if (provider_state.HasPendingCommand) ....
						} // if (!provider_state.IsProcessRequest)
					} catch (IOException e) {
						// If an IOException is generated, we must remove this provider from
						// the list.
						try {
							connection_state.Connection.Close();
						} catch (IOException e2) { /* ignore */ }
						server_connections_list.RemoveAt(i);

						// This happens if the connection closes.
						parent.controller.Debug.Write(DebugLevel.Information, this, "IOException generated while checking connections, " +
						                                          "removing provider.");
						parent.controller.Debug.WriteException(DebugLevel.Information, e);
					}
				}
			}

			private class ProcessRequestEventImpl {
				public ProcessRequestEventImpl(ServerFarmer farmer, ServerConnectionState current_state) {
					this.farmer = farmer;
					this.current_state = current_state;
				}

				private readonly ServerFarmer farmer;
				private readonly ServerConnectionState current_state;

				public void Execute(object sender, EventArgs args) {
					try {
						// Process the next request that's pending.
						current_state.Connection.ProcessRequest();
					} catch (IOException ex) {
						farmer.parent.controller.Debug.WriteException(DebugLevel.Information, ex);
					} finally {
						// Then clear the state
						// This makes sure that this provider may accept new
						// commands again.
						current_state.ClearInternal();
					}
				}
			}

			/// <summary>
			/// Performs a ping on a single random connection.
			/// </summary>
			/// <remarks>
			/// If the ping fails then the connection is closed.
			/// </remarks>
			private void DoPings() {
				int len = server_connections_list.Count;
				if (len == 0) {
					if (DISPLAY_STATS) {
						Console.Out.Write("[TCPServer Stats] ");
						Console.Out.WriteLine("Ping tried but no connections.");
					}
					return;
				}
				Random rnd = new Random();
				int i = (int)(rnd.Next() * len);

				if (DISPLAY_STATS) {
					Console.Out.Write("[TCPServer Stats] ");
					Console.Out.Write("Pinging client ");
					Console.Out.Write(i);
					Console.Out.Write(".");
				}

				ServerConnectionState connection_state =
								 (ServerConnectionState)server_connections_list[i];

				// Is this provider not currently processing a command?
				if (!connection_state.IsProcessingRequest) {
					// Don't let a command interrupt the ping.
					connection_state.SetProcessingRequest();

					// ISSUE: Pings are executed under 'null' user and database...
					//PingCallback pingCallback  = new PingCallback(this, connection_state);
					parent.controller.Execute(delegate {
					                          	try {
					                          		// Ping the client? - This closes the provider if the
					                          		// ping fails.
					                          		connection_state.Connection.Ping();
					                          	} catch (IOException ex) {
					                          		// Close connection
					                          		try {
					                          			connection_state.Connection.Close();
					                          		} catch (IOException e2) {
					                          			/* ignore */
					                          		}
					                          		parent.controller.Debug.Write(DebugLevel.Alert, this, "Closed because ping failed.");
					                          		parent.controller.Debug.WriteException(DebugLevel.Alert, ex);
					                          	} finally {
					                          		connection_state.ClearProcessingRequest();
					                          	}

					                          });

				} // if (!provider_state.isProcessRequest())
			}

			//private class PingCallback {
			//    public PingCallback(ServerFarmer farmer, ServerConnectionState connection_state) {
			//        this.connection_state = connection_state;
			//        this.farmer = farmer;
			//    }

			//    private readonly ServerFarmer farmer;
			//    private readonly ServerConnectionState connection_state;

			//    public void Execute(object sender, EventArgs args) {
			//        try {
			//            // Ping the client? - This closes the provider if the
			//            // ping fails.
			//            connection_state.Connection.Ping();
			//        } catch (IOException ex) {
			//            // Close connection
			//            try {
			//                connection_state.Connection.Close();
			//            } catch (IOException e2) { /* ignore */ }
			//            farmer.parent.controller.Debug.Write(DebugLevel.Alert, this, "Closed because ping failed.");
			//            farmer.parent.controller.Debug.WriteException(DebugLevel.Alert, ex);
			//        } finally {
			//            connection_state.ClearProcessingRequest();
			//        }
			//    }
			//}


			/// <summary>
			/// Displays statistics about the server.
			/// </summary>
			private void DisplayStatistics() {
				if (DISPLAY_STATS) {
					if (stat_display == 0) {
						stat_display = 500;
						Console.Out.Write("[TCPServer Stats] ");
						Console.Out.Write(commands_run);
						Console.Out.Write(" run, ");
						Console.Out.Write(commands_waited);
						Console.Out.Write(" wait, ");
						Console.Out.Write(server_connections_list.Count);
						Console.Out.Write(" worker count");
						Console.Out.WriteLine();
					} else {
						--stat_display;
					}
				}
			}

			/// <summary>
			/// Stops the farmer thread.
			/// </summary>
			public void Close() {
				lock (this) {
					farmer_closed = true;
				}
			}

			/**
			 * The Runnable method of the farmer thread.
			 */

			private void Run() {
				int yield_count = 0;
				DateTime do_ping_time = DateTime.Now.AddMilliseconds(PING_BREAK);
				int ping_count = 200;

				int method_poll_wait_time = poll_wait_time;

				parent.controller.Debug.Write(DebugLevel.Message, this, "Polling frequency: " + method_poll_wait_time + "ms.");

				while (true) {
					try {

						// First, determine if there are any pending service providers
						// waiting to be established.
						if (parent.pending_connections_list.Count > 0) {
							EstablishPendingConnections();
						}
						CheckCurrentConnections();

						// Is it time to ping the clients?
						--ping_count;
						if (ping_count <= 0) {
							ping_count = 2000;
							DateTime current_time = DateTime.Now;
							if (current_time > do_ping_time) {
								// Randomly ping
								DoPings();
								do_ping_time = current_time.AddMilliseconds(PING_BREAK);
							}
						}

						if (yield_count <= 0) {
							lock (this) {
								// Wait for 3ms to give everything room to breath
								Monitor.Wait(this, method_poll_wait_time);
								yield_count = 3;
							}
						} else {
							lock (this) {
								// Exit if the farmer thread has been closed...
								if (farmer_closed == true) {
									return;
								}
							}
							//TODO: check this... Thread.yield();
							Thread.Sleep(0);
							--yield_count;
						}

						// Print out connection statistics every so often
						DisplayStatistics();

					} catch (Exception e) {
						parent.controller.Debug.Write(DebugLevel.Error, this, "Connection Pool Farmer Error");
						parent.controller.Debug.WriteException(e);

						// Wait for two seconds (so debug log isn't spammed)
						lock (this) {
							try {
								Monitor.Wait(this, 2000);
							} catch (ThreadInterruptedException e2) { /* ignore */ }
						}

					}
				}
			}

		};


		/// <summary>
		/// This contains state information about a <see cref="IServerConnection"/> that 
		/// is being maintained by the server.
		/// </summary>
		private sealed class ServerConnectionState {
			private IServerConnection connection;
			private bool is_processing_request;
			private bool is_pending_command;

			/**
			 * The Constructor.
			 */
			internal ServerConnectionState(IServerConnection connection) {
				this.connection = connection;
				ClearInternal();
				//      is_establish = true;
			}

			/// <summary>
			/// Sets the various states to true.
			/// </summary>
			public void SetProcessingRequest() {
				lock (this) {
					is_processing_request = true;
				}
			}

			public void SetPendingCommand() {
				lock (this) {
					is_pending_command = true;
				}
			}

			/// <summary>
			/// Clears the internal state.
			/// </summary>
			public void ClearInternal() {
				lock (this) {
					is_processing_request = false;
					is_pending_command = false;
				}
			}

			/// <summary>
			/// Clears the flag that says we are processing a request.
			/// </summary>
			public void ClearProcessingRequest() {
				lock (this) {
					is_processing_request = false;
				}
			}

			/**
			 * Queries the internal state.
			 */

			public IServerConnection Connection {
				get {
					lock (this) {
						return connection;
					}
				}
			}

			public bool IsProcessingRequest {
				get {
					lock (this) {
						return is_processing_request;
					}
				}
			}

			public bool HasPendingCommand {
				get {
					lock (this) {
						return is_pending_command;
					}
				}
			}
		}
	}
}