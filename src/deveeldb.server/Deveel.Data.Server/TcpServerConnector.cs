using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;

using Deveel.Data.Protocol;

namespace Deveel.Data.Server {
	public sealed class TcpServerConnector : ServerConnector {
		private readonly ConnectionEndPoint localEndPoint;

		public TcpServerConnector(IDatabaseHandler databaseHandler, ConnectionEndPoint localEndPoint) 
			: base(databaseHandler) {
			this.localEndPoint = localEndPoint;
		}

		public override ConnectionEndPoint LocalEndPoint {
			get { return localEndPoint; }
		}

		protected override IServerMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
			throw new NotImplementedException();
		}

		public override ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties) {
			object objIpAddress;
			if (!properties.TryGetValue("address", out objIpAddress))
				throw new InvalidOperationException();

			return new ConnectionEndPoint(KnownConnectionProtocols.TcpIp, objIpAddress.ToString());
		}
	}
}
