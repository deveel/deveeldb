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
using System.Runtime.Serialization;

namespace Deveel.Data.Protocol {
	[Serializable]
	public class NetworkEnvelope : IMessageEnvelope, ISerializable {
		private readonly Dictionary<string, object> metadata;
 
		public NetworkEnvelope(int dispatchId, IMessage message) {
			Message = message;

			metadata = new Dictionary<string, object>();
		}

		protected NetworkEnvelope(SerializationInfo info, StreamingContext context) {
			DispatchId = info.GetInt32(NetworkEnvelopeMetadataKeys.DispatchId);
			IssueDate = new DateTime(info.GetInt64(NetworkEnvelopeMetadataKeys.IssueDate));
			Error = (ServerError) info.GetValue("Error", typeof (ServerError));
			Message = (IMessage) info.GetValue("Message", typeof (IMessage));
		}

		IDictionary<string, object> IMessageEnvelope.Metadata {
			get { return metadata; }
		}

		protected object GetMetadata(string key) {
			object value;
			if (!metadata.TryGetValue(key, out value))
				return null;

			return value;
		}

		protected void SetMetadata(string key, object value) {
			metadata[key] = value;
		}

		public IMessage Message { get; private set; }

		public ServerError Error { get; set; }

		public int DispatchId {
			get { return (int) GetMetadata(NetworkEnvelopeMetadataKeys.DispatchId); }
			set { SetMetadata(NetworkEnvelopeMetadataKeys.DispatchId, value); }
		}

		public DateTime IssueDate {
			get { return (DateTime) GetMetadata(NetworkEnvelopeMetadataKeys.IssueDate); }
			set { SetMetadata(NetworkEnvelopeMetadataKeys.IssueDate, value);}
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue(NetworkEnvelopeMetadataKeys.DispatchId, DispatchId);
			info.AddValue(NetworkEnvelopeMetadataKeys.IssueDate, IssueDate.Ticks);
			info.AddValue("Error", Error);
			info.AddValue("Message", Message);
		}
	}
}