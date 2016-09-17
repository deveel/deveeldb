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

using Deveel.Data.Sql.Triggers;
using System.Text;

using Deveel.Data.Client;
using Deveel.Data.Store;

namespace Deveel.Data.Protocol {
	public abstract class ClientConnector : IClientConnector {
		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {

		}

		public ConnectorState CurrentState { get; private set; }

		public virtual ConnectionEndPoint LocalEndPoint { get; private set; }

		protected abstract ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties);

		ConnectionEndPoint IClientConnector.MakeEndPoint(IDictionary<string, object> properties) {
			return MakeEndPoint(properties);
		}

		IMessageProcessor IConnector.CreateProcessor() {
			return new ClientMessageProcessor(this);
		}

		IMessageEnvelope IConnector.CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
			return CreateEnvelope(metadata, message);
		}

		protected abstract IMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message);

		ITriggerChannel IConnector.CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			return CreateTriggerChannel(triggerName, objectName, eventType);
		}

		protected abstract ITriggerChannel CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType);

		protected abstract ILargeObjectChannel CreateObjectChannel(ObjectId objId);

		ILargeObjectChannel IConnector.CreateObjectChannel(ObjectId objId) {
			return CreateObjectChannel(objId);
		}

		protected abstract IMessageEnvelope SendEnvelope(IMessageEnvelope envelope);

		protected virtual IMessage OpenEnvelope(IMessageEnvelope envelope) {
			return envelope.Message;
		}

		protected virtual void OnMessageReceived(IMessage message) {
		}

		void IClientConnector.SetEncrypton(EncryptionData encryptionData) {
			OnSetEncryption(encryptionData);
		}

		protected virtual void OnSetEncryption(EncryptionData encryptionData) {
		}

		class ClientMessageProcessor : IMessageProcessor {
			private readonly ClientConnector connector;

			public ClientMessageProcessor(ClientConnector connector) {
				this.connector = connector;
			}

			public IMessageEnvelope ProcessMessage(IMessageEnvelope message) {
				var response = connector.SendEnvelope(message);
				if (response == null)
					throw new InvalidOperationException("Unable to obtain a response from the server.");

				if (response.Error != null)
				{
					var sb = new StringBuilder();
					if (null != response.Error) {
						sb.Append (response.Error.ErrorMessage);
						sb.AppendFormat (", ErrorCode: {0}", response.Error.ErrorCode);
						sb.AppendFormat (", ErrorClass: {0}", response.Error.ErrorClass);
					}
					else
						sb.Append ("response.Error == null");
					// ServerError class is not an exception!!! so it have no stack trace...
					throw new DeveelDbServerException (sb.ToString(), -1, -1);
				}

				var content = connector.OpenEnvelope(response);
				connector.OnMessageReceived(content);
				return response;
			}
		}
	}
}