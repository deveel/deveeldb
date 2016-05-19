using System;
using System.Net;
using System.Net.Sockets;

using Deveel.Data.Configuration;

namespace Deveel.Data.Server {
	public sealed class TcpService {
		private TcpListener listener;

		public const uint DefaultPort = 27888;

		public TcpService(IConfiguration configuration, IDatabaseHandler handler) {
			if (handler == null)
				throw new ArgumentNullException("handler");

			Configuration = configuration;
			DatabaseHandler = handler;
		}

		public IConfiguration Configuration { get; private set; }

		public IDatabaseHandler DatabaseHandler { get; private set; }

		public bool IsListening { get; private set; }

		public void Start() {
			var hostName = Dns.GetHostName();
			var ipAddresses = Dns.GetHostAddresses(hostName);

			var port = Configuration.GetUInt32("server.tcp.port", DefaultPort);

			Listen(ipAddresses[0], port);
		}

		private void Listen(IPAddress address, uint port) {
			try {
				listener = new TcpListener(new IPEndPoint(address, (int)port));
				listener.Start();
			} catch (Exception ex) {
				//TODO: log the exception in a logger...
				throw;
			}

			IsListening = true;

			while (IsListening) {
				try {
					var socket = listener.AcceptSocket();

					//TODO: wrap it into a connector
					//TODO: add the connector to the list
				} catch (Exception ex) {
					// TODO: register the error in a logger...
				}
			}
		}

		public void Stop() {
			try {
				IsListening = false;

				//TODO: close all the connectors open...
				//TODO: invoke a SHUTDOWN on all databases
			} catch (Exception ex) {
				// TODO: register the error in a logger...
			} finally {
				if (listener != null)
					listener.Stop();
			}
		}

		public void Shutdown() {
			Stop();
		}
	}
}
