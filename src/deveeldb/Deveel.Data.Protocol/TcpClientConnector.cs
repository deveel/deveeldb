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
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Deveel.Data.Protocol {
	public sealed class TcpClientConnector : NetworkClientConnector {
		public const int DefaultPort = 6669;

		public override ConnectionEndPoint LocalEndPoint {
			get { return new ConnectionEndPoint(KnownConnectionProtocols.TcpIp, Dns.GetHostName()); }
		}

		public override ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties) {
			object hostObj, portObj;
			if (!properties.TryGetValue("Host", out hostObj))
				throw new InvalidOperationException("Could not find the host address.");

			var host = (string) hostObj;
			var port = DefaultPort;

			if (properties.TryGetValue("Port", out portObj))
				port = (int) portObj;

			var address  = new StringBuilder(host);
			if (port > 0 && port != DefaultPort)
				address.AppendFormat(":{0}", port);

			return new ConnectionEndPoint(KnownConnectionProtocols.TcpIp, address.ToString());
		}

		private EndPoint ParseEndPoint(string s) {
			string address = s;
			int port = DefaultPort;

			var sepIndex = address.IndexOf(':');
			if (sepIndex != -1) {
				string sPort = address.Substring(sepIndex + 1);
				if (!Int32.TryParse(sPort, out port))
					throw new FormatException();

				address = address.Substring(0, sepIndex);
			}

			return new IPEndPoint(IPAddress.Parse(address), port);
		}

		protected override NetworkStream CreateNetworkStream(ConnectionEndPoint remoteEndPoint, FileAccess access) {
			if (remoteEndPoint == null)
				throw new ArgumentNullException("remoteEndPoint");
			if (remoteEndPoint.Protocol != KnownConnectionProtocols.TcpIp)
				throw new ArgumentException();

			var endPoint = ParseEndPoint(remoteEndPoint.Address);

			var sockect = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
			sockect.SendTimeout = Timeout;
			sockect.ReceiveTimeout = Timeout;
			sockect.Connect(endPoint);

			return new NetworkStream(sockect, access, true);
		}
	}
}