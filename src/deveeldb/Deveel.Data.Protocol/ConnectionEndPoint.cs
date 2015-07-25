// 
//  Copyright 2010-2015 Deveel
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
//

using System;

namespace Deveel.Data.Protocol {
	public sealed class ConnectionEndPoint {
		public ConnectionEndPoint(string protocol, string address) {
			if (String.IsNullOrEmpty(protocol))
				throw new ArgumentNullException("protocol");
			if (String.IsNullOrEmpty(address))
				throw new ArgumentNullException("address");

			Address = address;
			Protocol = protocol;
		}

		public string Protocol { get; private set; }

		public string Address { get; private set; }

		public static readonly ConnectionEndPoint Embedded = new ConnectionEndPoint(KnownConnectionProtocols.Local, "%");

		public static ConnectionEndPoint Parse(string s) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");

			var index = s.IndexOf(':');
			if (index == -1)
				throw new FormatException();

			var protocol = s.Substring(0, index).Trim();
			var address = s.Substring(index + 1).Trim();

			if (String.IsNullOrEmpty(protocol))
				throw new FormatException();

			if (String.IsNullOrEmpty(address))
				throw new FormatException();

			return new ConnectionEndPoint(protocol, address);
		}
	}
}