using System;
using System.Collections;
using System.IO;
using System.Threading;

using Deveel.Data.Control;
using Deveel.Diagnostics;

namespace Deveel.Data.Server {
	/// <summary>
	/// A multi-threaded implementation of a connection pool server.
	/// </summary>
	/// <remarks>
	/// This starts a new thread for each connection made and processes each 
	/// command as they arrive.
	/// </remarks>
	sealed class MultiThreadedConnectionPoolServer : IConnectionPoolServer {
		/// <summary>
		/// If this is set to true then the server periodically outputs statistics
		/// about the connections.
		/// </summary>
		private const bool DISPLAY_STATS = false;

		/// <summary>
		/// The <see cref="Database"/> parent.
		/// </summary>
		private readonly TcpServerController controller;

		/// <summary>
		/// The list of all threads that were created to deal with incoming commands.
		/// </summary>
		private readonly ArrayList client_threads;


		internal MultiThreadedConnectionPoolServer(TcpServerController controller) {
			this.controller = controller;
			client_threads = new ArrayList();
		}

		/// <inheritdoc/>
		/// <remarks>
		/// We then cycle through these connections determining whether any commands are 
		/// pending. If a command is pending we spawn off a worker thread to do the task.
		/// </remarks>
		public void AddConnection(IServerConnection connection) {
			ClientThread client_thread = new ClientThread(this, connection);
			lock (client_threads) {
				client_threads.Add(client_thread);
			}
			client_thread.Start();
		}

		public void Close() {
			lock (client_threads) {
				int size = client_threads.Count;
				for (int i = 0; i < size; ++i) {
					((ClientThread)client_threads[i]).Close();
				}
			}
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// This thread blocks waiting for a complete command to arrive from the
		/// client it is connected to.
		/// </summary>
		private class ClientThread {
			private readonly MultiThreadedConnectionPoolServer parent;
			private readonly Thread thread;

			/// <summary>
			/// The <see cref="IServerConnection"/> object being serviced 
			/// by this thread.
			/// </summary>
			private readonly IServerConnection server_connection;

			/// <summary>
			/// If this is set to true, the thread run method should close off.
			/// </summary>
			private bool client_closed;

			/// <summary>
			/// This is set to true if we are processing a request from the client.
			/// </summary>
			private bool processing_command;

			public ClientThread(MultiThreadedConnectionPoolServer parent, IServerConnection connection) {
				this.parent = parent;
				thread = new Thread(new ThreadStart(run));
				//      setPriority(NORM_PRIORITY - 1);
				thread.Name = "Mckoi - Client Connection";

				this.server_connection = connection;
				client_closed = false;
				processing_command = false;
			}

			/// <summary>
			/// Checks each connection in the <see cref="server_connection"/> 'service_connection_list' list.
			/// </summary>
			/// <remarks>
			/// If there is a command pending, and any previous commands on this connection have 
			/// completed, then this will spawn off a new process to deal with the command.
			/// </remarks>
			private void CheckCurrentConnection() {
				try {
					// Wait if we are processing a command
					lock (this) {
						while (processing_command) {
							if (client_closed)
								return;
							// Wait 2 minutes just to make sure we don't miss a poll,
							Monitor.Wait(this, 120000);
						}
					}

					// Block until a complete command is available
					server_connection.BlockForRequest();
					processing_command = true;
					Database database = server_connection.Database;
					database.Execute(null, null, new EventHandler(ProcessRequest));
				} catch (IOException e) {
					// If an IOException is generated, we must remove this provider from
					// the list.
					Close();

					// This happens if the connection closes.
					Debug.Write(DebugLevel.Information, this, "IOException generated while checking connections, removing provider.");
					Debug.WriteException(DebugLevel.Information, e);
				}

			}

			private void ProcessRequest(object sender, EventArgs e) {
				try {
					// Process the next request that's pending.
					server_connection.ProcessRequest();
				} catch (IOException ex) {
					Debug.WriteException(DebugLevel.Information, ex);
				} finally {
					// Not processing a command anymore so notify the ClientThread
					processing_command = false;
					lock (this) {
						Monitor.PulseAll(this);
					}
				}
			}

			/// <summary>
			/// Call this method to stop the thread.
			/// </summary>
			public void Close() {
				lock (this) {
					client_closed = true;
					try {
						server_connection.Close();
					} catch (Exception e) {
						// ignore
					}
					Monitor.PulseAll(this);
				}
			}

			public void run() {
				while (true) {
					try {

						bool closed = false;
						lock (this) {
							closed = client_closed;
						}
						// Exit if the farmer thread has been closed...
						if (closed == true) {
							// Remove this thread from the list of client threads.
							lock (parent.client_threads) {
								parent.client_threads.Remove(this);
							}
							return;
						}

						CheckCurrentConnection();

					} catch (Exception e) {
						Debug.Write(DebugLevel.Error, this, "Connection Pool Farmer Error");
						Debug.WriteException(e);
					}
				}
			}

			public void Start() {
				thread.Start();
			}
		};
	}
}