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

using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Protocol {
	public abstract class ServerClientConnector : ClientConnector {
		protected ServerClientConnector(IServerConnector connector) {
			if (connector == null) 
				throw new ArgumentNullException("connector");

			ServerConnector = connector;
			ServerMessageProcessor = connector.CreateProcessor();
		}

		public override ConnectionEndPoint LocalEndPoint {
			get { return ServerConnector.LocalEndPoint; }
		}

		protected IMessageProcessor ServerMessageProcessor { get; private set; }

		protected IServerConnector ServerConnector { get; private set; }

		protected override ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties) {
			return ServerConnector.LocalEndPoint;
		}

		protected override ILargeObjectChannel CreateObjectChannel(long objectId) {
			return ServerConnector.CreateObjectChannel(objectId);
		}

		protected override ITriggerChannel CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			return ServerConnector.CreateTriggerChannel(triggerName, objectName, eventType);
		}

		protected override IMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
			return ServerConnector.CreateEnvelope(metadata, message);
		}

		protected override IMessageEnvelope SendEnvelope(IMessageEnvelope envelope) {
			return ServerMessageProcessor.ProcessMessage(envelope);
		}
	}
}