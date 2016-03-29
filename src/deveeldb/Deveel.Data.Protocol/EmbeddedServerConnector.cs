// 
//  Copyright 2010-2016 Deveel
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
using System.Collections.Generic;

using Deveel.Data;

namespace Deveel.Data.Protocol {
	public class EmbeddedServerConnector : ServerConnector {
		public EmbeddedServerConnector(IDatabaseHandler handler)
			: base(handler) {
		}

		public override ConnectionEndPoint LocalEndPoint {
			get { return ConnectionEndPoint.Embedded; }
		}

		public override ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties) {
			return ConnectionEndPoint.Embedded;
		}

		protected override IServerMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
			return EmbeddedMessageEnvelope.Create(metadata, message);
		}
	}
}