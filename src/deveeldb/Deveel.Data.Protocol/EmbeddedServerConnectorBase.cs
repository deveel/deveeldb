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

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Routines;
using Deveel.Diagnostics;

namespace Deveel.Data.Protocol {
	public abstract class EmbeddedServerConnectorBase : ServerConnector {
		protected EmbeddedServerConnectorBase(IDatabaseHandler handler)
			: base(handler) {
		}

		private void AssertNotDisposed() {
			if (CurrentState == ConnectorState.Disposed)
				throw new InvalidOperationException("The connector was disposed.");
		}

		private void AssertAuthenticated() {
			if (CurrentState != ConnectorState.Authenticated)
				throw new InvalidOperationException("The session was not authenticated.");
		}

		public override IMessageProcessor CreateProcessor() {
			AssertNotDisposed();
			return new EmbeddedMessageProcessor(this);
		}

		private int currentDispatchId;

		public override ConnectionEndPoint LocalEndPoint {
			get { return ConnectionEndPoint.Embedded; }
		}

		public override ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties) {
			return ConnectionEndPoint.Embedded;
		}

		protected override IServerMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
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

		protected override void OnTriggerFired(string triggerName, string triggerSource, TriggerEventType eventType, int count) {
		}

		//#region EmbeddedMessageProcessor

		//class EmbeddedMessageProcessor : IMessageProcessor {
		//	private readonly EmbeddedServerConnectorBase connector;

		//	public EmbeddedMessageProcessor(EmbeddedServerConnectorBase connector) {
		//		this.connector = connector;
		//	}

		//	private EmbeddedMessageEnvelope CreateEnvelope(int dispatchId, IMessage message) {
		//		return connector.CreateEnvelope(dispatchId, message);
		//	}

		//	private IMessageEnvelope ErrorResponse(int dispatchId, string errorMessagge) {
		//		return ErrorResponse(dispatchId, new AcknowledgeResponse(false), new Exception(errorMessagge));
		//	}

		//	private IMessageEnvelope ErrorResponse(int dispatchId, IMessage message, Exception error) {
		//		var envelope = CreateEnvelope(dispatchId, message);
		//		// TODO: catch DatabaseException with error class and code
		//		envelope.SetError(error);
		//		return envelope;
		//	}

		//	private IMessageEnvelope ProcessAuthenticate(AuthenticateRequest request) {
		//		var schema = request.DefaultSchema;
		//		if (String.IsNullOrEmpty(schema))
		//			// TODO: Load this dynamically ...
		//			schema = ConfigDefaultValues.DefaultSchema;

		//		if (!connector.Authenticate(schema, request.UserName, request.Password)) {
		//			// TODO: trap an exception from upper level and return it
		//			return ErrorResponse(-1, new AuthenticateResponse(false, -1), new Exception("Could not authenticate"));
		//		}

		//		connector.ChangeState(ConnectorState.Authenticated);

		//		// TODO: output UNIX time?
		//		return connector.CreateEnvelope(null, new AuthenticateResponse(true, DateTimeOffset.UtcNow.Ticks));
		//	}

		//	private IMessageEnvelope ProcessQuery(int dispatchId, QueryExecuteRequest request) {
		//		try {
		//			var response = connector.ExecuteQuery(request.Query.Text, request.Query.Parameters);
		//			return CreateEnvelope(dispatchId, new QueryExecuteResponse(response));
		//		} catch (Exception ex) {
		//			// TODO: Return an error message
		//			throw;
		//		}
		//	}

		//	private IMessageEnvelope ProcessDisposeResult(int dispatchId, DisposeResultRequest request) {
		//		connector.DisposeResult(request.ResultId);
		//		return CreateEnvelope(dispatchId, new AcknowledgeResponse(true));
		//	}

		//	private IMessageEnvelope ProcessConnect(ConnectRequest command) {
		//		try {
		//			connector.OpenConnector(command.RemoteEndPoint, command.DatabaseName);
		//			if (command.AutoCommit)
		//				connector.SetAutoCommit(command.AutoCommit);

		//			connector.SetIgnoreIdentifiersCase(command.IgnoreIdentifiersCase);
		//			connector.SetParameterStyle(command.ParameterStyle);

		//			return CreateEnvelope(-1, new ConnectResponse(true, connector.Database.Version.ToString(2)));
		//		} catch (Exception ex) {
		//			connector.Logger.Error(connector, "Error while opening a connection.");
		//			connector.Logger.Error(connector, ex);

		//			return ErrorResponse(-1, new ConnectResponse(false, connector.Database.Version.ToString(2)), ex);
		//		}
		//	}

		//	private IMessageEnvelope ProcessClose(int dispatchId) {
		//		connector.AssertAuthenticated();
		//		connector.CloseConnector();
		//		return CreateEnvelope(dispatchId, new AcknowledgeResponse(true));
		//	}

		//	private IMessageEnvelope ProcessBegin(int dispatchId) {
		//		connector.AssertAuthenticated();
		//		connector.BeginTransaction();
		//		return CreateEnvelope(dispatchId, new AcknowledgeResponse(true));
		//	}

		//	private IMessageEnvelope ProcessCommit(int dispatchId) {
		//		connector.AssertAuthenticated();
		//		connector.CommitTransaction();
		//		return CreateEnvelope(dispatchId, new AcknowledgeResponse(true));
		//	}

		//	private IMessageEnvelope ProcessRollback(int dispatchId) {
		//		connector.AssertAuthenticated();
		//		connector.RollbackTransaction();
		//		return CreateEnvelope(dispatchId, new AcknowledgeResponse(true));
		//	}

		//	private IMessageEnvelope ProcessMessage(int dispatchId, IMessage message) {
		//		if (message is ConnectRequest)
		//			return ProcessConnect((ConnectRequest)message);

		//		if (message is AuthenticateRequest)
		//			return ProcessAuthenticate((AuthenticateRequest) message);

		//		if (message is CloseCommand)
		//			return ProcessClose(dispatchId);

		//		if (message is QueryExecuteRequest)
		//			return ProcessQuery(dispatchId, (QueryExecuteRequest) message);
		//		if (message is QueryResultPartRequest)
		//			return ProcessQueryPart(dispatchId, (QueryResultPartRequest) message);
		//		if (message is DisposeResultRequest)
		//			return ProcessDisposeResult(dispatchId, (DisposeResultRequest) message);

		//		if (message is LargeObjectCreateRequest)
		//			return ProcessCreateStreamableObject(dispatchId, (LargeObjectCreateRequest) message);

		//		if (message is BeginRequest)
		//			return ProcessBegin(dispatchId);
		//		if (message is CommitRequest)
		//			return ProcessCommit(dispatchId);
		//		if (message is RollbackRequest)
		//			return ProcessRollback(dispatchId);

		//		return ErrorResponse(dispatchId, "Unable to process the message.");
		//	}

		//	private IMessageEnvelope ProcessCreateStreamableObject(int dispatchId, LargeObjectCreateRequest request) {
		//		var objId = connector.CreateStreamableObject(request.ReferenceType, request.ObjectLength);
		//		return CreateEnvelope(dispatchId,
		//			new LargeObjectCreateResponse(request.ReferenceType, request.ObjectLength, objId));
		//	}

		//	private IMessageEnvelope ProcessQueryPart(int dispatchId, QueryResultPartRequest request) {
		//		try {
		//			var part = connector.GetResultPart(request.ResultId, request.RowIndex, request.Count);
		//			return CreateEnvelope(dispatchId, new QueryResultPartResponse(request.ResultId, part));
		//		} catch (Exception) {
		//			// TODO: Return an error envelope
		//			throw;
		//		}
		//	}

		//	public IMessageEnvelope ProcessMessage(IMessageEnvelope message) {
		//		connector.AssertNotDisposed();

		//		var envelope = message as EmbeddedMessageEnvelope;
		//		if (envelope == null)
		//			throw new InvalidOperationException("The envelope was created in another context.");

		//		return ProcessMessage(envelope.DispatchId, envelope.Message);
		//	}
		//}

		//#endregion
	}
}