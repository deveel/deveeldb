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
using System.Text;

namespace Deveel.Data.Protocol {
	public sealed class EmbeddedMessageEnvelope : IServerMessageEnvelope {
		private static int dispatchCounter = -1;

		private EmbeddedMessageEnvelope(IDictionary<string, object> metadata, IMessage message) {
			Message = message;
			Metadata = metadata;

			object dispatchId;
			if (!metadata.TryGetValue("DispatchID", out dispatchId))
				throw new ArgumentException("Metadata must specify a Dispatch ID");

			DispatchId = (int) dispatchId;
		}

		public IDictionary<string, object> Metadata { get; private set; }

		public int DispatchId { get; private set; }

		public IMessage Message { get; private set; }

		public ServerError Error { get; private set; }

		public void SetError(Exception error) {
			var sb = new StringBuilder ();
			sb.Append (error.Message);
			sb.AppendLine ();
			sb.Append (error.ToString());
			// TODO: in another version there will be support for Class and Code of error
			Error = new ServerError(-1, -1, sb.ToString());
		}

		public static EmbeddedMessageEnvelope Create(int dispatchId, IMessage message) {
			var metadata = new Dictionary<string, object>();
			metadata["DispatchID"] = dispatchId;
			return new EmbeddedMessageEnvelope(metadata, message);
		}

		public static EmbeddedMessageEnvelope Create(IDictionary<string, object> metadata, IMessage message) {
			if (metadata == null)
				metadata = new Dictionary<string, object>();

			object dispatchId;
			if (!metadata.TryGetValue("DispatchID", out dispatchId))
				dispatchId = dispatchCounter++;

			metadata["DispatchID"] = dispatchId;
			return new EmbeddedMessageEnvelope(metadata, message);
		}
	}
}