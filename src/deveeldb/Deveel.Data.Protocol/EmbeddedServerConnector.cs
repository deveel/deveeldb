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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Protocol {
	public class EmbeddedServerConnector : ServerConnector {
		public EmbeddedServerConnector(IDatabase database)
			: base(database) {
		}

		private void AssertNotDisposed() {
			if (CurrentState == ConnectorState.Disposed)
				throw new InvalidOperationException("The connector was disposed.");
		}

		private void AssertAuthenticated() {
			if (CurrentState != ConnectorState.Authenticated)
				throw new InvalidOperationException("The session was not authenticated.");
		}

		private void AssertProcessing() {
			if (CurrentState != ConnectorState.Processing)
				throw new InvalidOperationException("Not processing.");
		}

		public override IMessageProcessor CreateProcessor() {
			AssertNotDisposed();
			return new EmbeddedMessageProcessor(this);
		}

		private int currentDispatchId;

		public override IMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
			AssertNotDisposed();

			if (message == null)
				throw new ArgumentNullException("message");

			if (metadata == null)
				metadata = new Dictionary<string, object>();

			object dispatchId;
			if (!metadata.TryGetValue("DispatchID", out dispatchId)) {
				metadata["DispatchID"] = ++currentDispatchId;
			}

			return new EmbeddedMessageEnvelope(metadata, message);
		}

		private EmbeddedMessageEnvelope CreateEnvelope(int dispatchId, IMessage message) {
			return (EmbeddedMessageEnvelope) CreateEnvelope(new Dictionary<string, object> {{"DispatchID", dispatchId}}, message);
		}

		public override IStreamableObjectChannel CreateChannel(long objectId) {
			var obj = GetStreamableObject(objectId);
			if (obj == null)
				throw new InvalidOperationException();

			return new DirectStreamableObjectChannel(obj);
		}

		#region DirectStreamableObjectChannel

		private class DirectStreamableObjectChannel : IStreamableObjectChannel {
			private readonly IRef obj;

			public DirectStreamableObjectChannel(IRef obj) {
				this.obj = obj;
			}

			public void Dispose() {
			}

			public long ObjectId {
				get { return obj.Id; }
			}

			public ReferenceType ReferenceType {
				get { return obj.Type; }
			}

			public long Length {
				get { return obj.RawSize; }
			}

			public void PushData(byte[] buffer, long offset, int length) {
				obj.Write(offset, buffer, length);
			}

			public byte[] ReadData(long offset, int length) {
				if (length > 512*1024)
					throw new DatabaseException("Request length exceeds 512 KB");

				try {
					// Read the blob part into the byte array.
					var blobPart = new byte[length];
					obj.Read(offset, blobPart, length);

					// And return as a StreamableObjectPart object.
					return blobPart;
				} catch (IOException e) {
					throw new DatabaseException("Exception while reading blob: " + e.Message, e);
				}
			}

			public void Flush() {
				obj.Complete();
			}
		}

		#endregion

		#region EmbeddedMessageEnvelope

		class EmbeddedMessageEnvelope : IMessageEnvelope {
			public EmbeddedMessageEnvelope(IDictionary<string, object> metadata, IMessage message) {
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

			public Exception Error { get; set; }
		}

		#endregion

		#region EmbeddedMessageProcessor

		class EmbeddedMessageProcessor : IMessageProcessor {
			private readonly EmbeddedServerConnector connector;

			public EmbeddedMessageProcessor(EmbeddedServerConnector connector) {
				this.connector = connector;
			}

			private EmbeddedMessageEnvelope CreateEnvelope(int dispatchId, IMessage message) {
				return connector.CreateEnvelope(dispatchId, message);
			}

			private IMessageEnvelope ErrorResponse(int dispatchId, string errorMessagge) {
				return ErrorResponse(dispatchId, new AcknowledgeResponse(false), new Exception(errorMessagge));
			}

			private IMessageEnvelope ErrorResponse(int dispatchId, IMessage message, Exception error) {
				var envelope = CreateEnvelope(dispatchId, message);
				envelope.Error = error;
				return envelope;				
			}

			private IMessageEnvelope ProcessAuthenticate(AuthenticateRequest request) {
				if (!connector.Authenticate(request.DefaultSchema, request.UserName, request.Password)) {
					// TODO: trap an exception from upper level and return it
					return ErrorResponse(-1, new AuthenticateResponse(false, -1), new Exception("Could not authenticate"));
				}

				connector.ChangeState(ConnectorState.Authenticated);

				// TODO: output UNIX time?
				return connector.CreateEnvelope(null, new AuthenticateResponse(true, DateTimeOffset.UtcNow.Ticks));
			}

			private IMessageEnvelope ProcessQuery(int dispatchId, QueryExecuteRequest request) {
				try {
					var response = connector.ExecuteQuery(request.Text, request.Parameters);
					return CreateEnvelope(dispatchId, new QueryExecuteResponse(response));
				} catch (Exception ex) {
					// TODO: Return an error message
					throw;
				}
			}

			private IMessageEnvelope ProcessDisposeResult(int dispatchId, DisposeResultRequest request) {
				connector.DisposeResult(request.ResultId);
				return CreateEnvelope(dispatchId, new AcknowledgeResponse(true));
			}

			private IMessageEnvelope ProcessOpen(int dispatchId) {
				connector.AssertAuthenticated();
				connector.OpenConnector();
				return CreateEnvelope(dispatchId, new AcknowledgeResponse(true));
			}

			private IMessageEnvelope ProcessClose(int dispatchId) {
				connector.AssertProcessing();
				connector.CloseConnector();
				return CreateEnvelope(dispatchId, new AcknowledgeResponse(true));
			}

			private IMessage ProcessMessage(int dispatchId, IMessage message) {
				if (message is AuthenticateRequest)
					return ProcessAuthenticate((AuthenticateRequest) message);

				if (message is OpenCommand)
					return ProcessOpen(dispatchId);
				if (message is CloseCommand)
					return ProcessClose(dispatchId);

				if (message is QueryExecuteRequest)
					return ProcessQuery(dispatchId, (QueryExecuteRequest) message);
				if (message is QueryResultPartRequest)
					return ProcessQueryPart(dispatchId, (QueryResultPartRequest) message);
				if (message is DisposeResultRequest)
					return ProcessDisposeResult(dispatchId, (DisposeResultRequest) message);

				if (message is StreamableObjectCreateRequest)
					return ProcessCreateStreamableObject(dispatchId, (StreamableObjectCreateRequest) message);
				if (message is StreamableObjectDisposeRequest)
					return ProcessDisposeStreamableObject(dispatchId, (StreamableObjectDisposeRequest) message);

				return ErrorResponse(dispatchId, "Unable to process the message.");
			}

			private IMessageEnvelope ProcessDisposeStreamableObject(int dispatchId, StreamableObjectDisposeRequest request) {
				bool result = connector.DisposeStreamableObject(request.ObjectId);
				return CreateEnvelope(dispatchId, new AcknowledgeResponse(result));
			}

			private IMessageEnvelope ProcessCreateStreamableObject(int dispatchId, StreamableObjectCreateRequest request) {
				var objId = connector.CreateStreamableObject(request.ReferenceType, request.ObjectLength);
				return CreateEnvelope(dispatchId,
					new StreamableObjectCreateResponse(request.ReferenceType, request.ObjectLength, objId));
			}

			private IMessageEnvelope ProcessQueryPart(int dispatchId, QueryResultPartRequest request) {
				try {
					var part = connector.GetResultPart(request.ResultId, request.RowIndex, request.Count);
					return CreateEnvelope(dispatchId, new QueryResultPartResponse(request.ResultId, part));
				} catch (Exception) {
					// TODO: Return an error envelope
					throw;
				}
			}

			public IMessage ProcessMessage(IMessage message) {
				connector.AssertNotDisposed();

				if (message is IMessageEnvelope) {
					var envelope = message as EmbeddedMessageEnvelope;
					if (envelope == null)
						throw new InvalidOperationException("The envelope was created in another context.");

					return ProcessMessage(envelope.DispatchId, envelope.Message);
				}

				return ProcessMessage(-1, message);
			}
		}

		#endregion
	}
}