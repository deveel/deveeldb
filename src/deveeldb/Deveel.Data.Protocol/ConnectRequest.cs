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

using Deveel.Data.Sql;

namespace Deveel.Data.Protocol {
	[Serializable]
	public sealed class ConnectRequest : IMessage {
		public ConnectRequest(ConnectionEndPoint localEndPoint, ConnectionEndPoint remoteEndPoint) {
			if (localEndPoint == null) 
				throw new ArgumentNullException("localEndPoint");
			if (remoteEndPoint == null)
				throw new ArgumentNullException("remoteEndPoint");

			LocalEndPoint = localEndPoint;
			RemoteEndPoint = remoteEndPoint;
		}

		public ConnectionEndPoint LocalEndPoint { get; private set; }

		public ConnectionEndPoint RemoteEndPoint { get; private set; }

		public string DatabaseName { get; set; }

		public bool AutoCommit { get; set; }

		public bool IgnoreIdentifiersCase { get; set; }

		public QueryParameterStyle ParameterStyle { get; set; }

		public int Timeout { get; set; }
	}
}