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
using System.Data;
using System.IO;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

using Deveel.Data.Client;
using Deveel.Data.Routines;

namespace Deveel.Data.Protocol {
	public abstract class NetworkClientConnector : IClientConnector {
		private readonly object channelLock = new object();

		private Thread envelopeReceiver;
		private List<IMessageEnvelope> envelopes;

		protected NetworkClientConnector() {
			envelopeReceiver = new Thread(ReceiveEnvelopes) {
				IsBackground = true,
				Name = "DeveelDB Network Client Envelope Receiver",
				Priority = ThreadPriority.AboveNormal
			};

			envelopes = new List<IMessageEnvelope>();
		}

		~NetworkClientConnector() {
			Dispose(false);
		}

		protected int Timeout { get; set; }

		public void Dispose() {
			try {
				Dispose(true);
			} catch (Exception) {
				// we ignore any exception at this point
			} finally {
				ChangeState(ConnectorState.Disposed);
			}

			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				try {
					Close();

					if (envelopeReceiver != null) {
						try {
							envelopeReceiver.Abort();
							envelopeReceiver = null;
						} catch (Exception) {

							throw;
						}
					}
				} catch (Exception) {
				}

				if (InputStream != null)
					InputStream.Dispose();
				if (OutputStream != null)
					OutputStream.Dispose();

				OutputStream = null;
				InputStream = null;

				ChangeState(ConnectorState.Disposed);
			}
		}

		private Stream InputStream { get; set; }

		private Stream OutputStream { get; set; }

		public ConnectorState CurrentState { get; private set; }

		public abstract ConnectionEndPoint LocalEndPoint { get; }

		public ConnectionEndPoint RemoteEndPoint { get; private set; }

		private void AssertNotDisposed() {
			if (CurrentState == ConnectorState.Disposed)
				throw new ObjectDisposedException(GetType().AssemblyQualifiedName);
		}

		private void AssertOpen() {
			if (CurrentState != ConnectorState.Open)
				throw new InvalidOperationException();
		}

		protected abstract NetworkStream CreateNetworkStream(ConnectionEndPoint remoteEndPoint, FileAccess access);

		protected void ChangeState(ConnectorState newState) {
			AssertNotDisposed();
			CurrentState = newState;
		}

		protected void OpenConnector(ConnectionEndPoint remoteEndPoint) {
			try {
				RemoteEndPoint = remoteEndPoint;
				var readStream = CreateNetworkStream(remoteEndPoint, FileAccess.Read);
				var writeStream = CreateNetworkStream(remoteEndPoint, FileAccess.Write);

				InputStream = new BufferedStream(readStream, 1024*3);
				OutputStream = new BufferedStream(writeStream, 1024*3);

				OnConnectorOpen();
				ChangeState(ConnectorState.Open);

				envelopeReceiver.Start();
			} catch (Exception ex) {
				//TODO: log somehwere ...
				throw;
			}
		}

		protected void Close() {
			try {
				ChangeState(ConnectorState.Closed);

				if (envelopeReceiver != null &&
				    envelopeReceiver.ThreadState == ThreadState.Running) {
					envelopeReceiver.Join(1000);
					envelopeReceiver = null;
				}

				if (InputStream != null)
					InputStream.Close();
				if (OutputStream != null)
					OutputStream.Close();
			} catch (Exception) {

				throw;
			}
		}

		protected virtual void OnAuthenticated(string username, long timeStamp) {
			// TODO: make something with username and timeStamp?

			ChangeState(ConnectorState.Authenticated);
		}

		protected virtual void OnConnectorOpen() {
		}

		public abstract ConnectionEndPoint MakeEndPoint(IDictionary<string, object> properties);

		public virtual IMessageProcessor CreateProcessor() {
			return new ClientProcessor(this);
		}

		public virtual IMessageEnvelope CreateEnvelope(IDictionary<string, object> metadata, IMessage message) {
			int dispatchId = ExtractDispatchId(metadata);
			var envelope = new NetworkEnvelope(dispatchId, message);
			envelope.IssueDate = DateTime.UtcNow;
			return envelope;
		}

		protected virtual IMessage OnProcessServerResponse(IMessageEnvelope envelope) {
			if (envelope == null)
				return null;
			if (envelope.Error != null)
				throw new DeveelDbException(envelope.Error.ErrorMessage, envelope.Error.ErrorClass, envelope.Error.ErrorCode);

			return envelope.Message;
		}

		public IStreamableObjectChannel CreateObjectChannel(long objectId, ObjectPersistenceType persistence) {
			throw new NotImplementedException();
		}

		public ITriggerChannel CreateTriggerChannel(string triggerName, string objectName, TriggerEventType eventType) {
			throw new NotImplementedException();
		}

		private ICryptoTransform SelectHashAlgorithm(string name, byte[] key, byte[] iv, FileAccess access) {
			if (String.Equals(name, EncryptionAlgorithms.HmacMd5, StringComparison.OrdinalIgnoreCase))
				return new HMACMD5(key);
			if (String.Equals(name, EncryptionAlgorithms.HmacSha256, StringComparison.OrdinalIgnoreCase))
				return new HMACSHA256(key);
			if (String.Equals(name, EncryptionAlgorithms.HmacSha512, StringComparison.OrdinalIgnoreCase))
				return new HMACSHA512(key);

			if (String.Equals(name, EncryptionAlgorithms.Des, StringComparison.OrdinalIgnoreCase)) {
				var des = new DESCryptoServiceProvider();
				if (access == FileAccess.Read)
					return des.CreateDecryptor(key, iv);
				if (access == FileAccess.Write)
					return des.CreateEncryptor(key, iv);
			}
			if (String.Equals(name, EncryptionAlgorithms.TripleDes, StringComparison.OrdinalIgnoreCase)) {
				var des = new TripleDESCryptoServiceProvider();
				if (access == FileAccess.Read)
					return des.CreateDecryptor(key, iv);
				if (access == FileAccess.Write)
					return des.CreateEncryptor(key, iv);
			}

			throw new NotSupportedException();
		}

		public void SetEncrypton(EncryptionData encryptionData) {
			lock (channelLock) {
				var key = Encoding.Unicode.GetBytes(encryptionData.Key);
				var iv = Encoding.Unicode.GetBytes(encryptionData.IV);
				var readHash = SelectHashAlgorithm(encryptionData.HashAlgorithm, key, iv, FileAccess.Read);
				var writeHash = SelectHashAlgorithm(encryptionData.HashAlgorithm, key, iv, FileAccess.Write);

				InputStream = new CryptoStream(InputStream, readHash, CryptoStreamMode.Read);
				OutputStream = new CryptoStream(OutputStream, writeHash, CryptoStreamMode.Write);
			}
		}

		protected virtual byte[] SerializeEnvelope(IMessageEnvelope envelope) {
			using (var stream = new MemoryStream()) {
				var formatter = new BinaryFormatter();
				formatter.Serialize(stream, envelope);
				stream.Flush();
				return stream.ToArray();
			}
		}

		protected void SendEnvelope(IMessageEnvelope envelope) {
			lock (channelLock) {
				var bytes = SerializeEnvelope(envelope);
				OutputStream.Write(bytes, 0, bytes.Length);
				OutputStream.Flush();
			}
		}

		protected virtual IMessageEnvelope DeserializeEnvelope(byte[] bytes) {
			using (var stream = new MemoryStream(bytes, false)) {
				var formatter = new BinaryFormatter();
				return (IMessageEnvelope) formatter.Deserialize(stream);
			}
		}

		private IMessageEnvelope ReceiveEnvelope(int timeout) {
			lock (channelLock) {
				using (var input = new BinaryReader(InputStream)) {
					try {
						int commandLength = input.ReadInt32();
						var buf = new byte[commandLength];
						input.Read(buf, 0, commandLength);
						return DeserializeEnvelope(buf);
					} catch (Exception) {
						//TODO: log ...
						throw;
					}
				}
			}
		}

		private static int ExtractDispatchId(IDictionary<string, object> metadata) {
			if (metadata == null || metadata.Count == 0)
				return -1;

			object id;
			if (!metadata.TryGetValue(NetworkEnvelopeMetadataKeys.DispatchId, out id))
				return -1;

			return (int) id;
		}

		protected virtual bool ShouldReceive(IDictionary<string, object> senderMetadata, IMessageEnvelope envelope) {
			var senderId = ExtractDispatchId(senderMetadata);
			var envelopeId = ExtractDispatchId(envelope.Metadata);
			return senderId == envelopeId;
		}

		private IMessageEnvelope ReceiveResponse(int timeout, IDictionary<string, object> senderMetadata) {
			DateTime timeIn = DateTime.Now;
			DateTime timeOutHigh = timeIn + new TimeSpan(((long) timeout*1000)*TimeSpan.TicksPerMillisecond);

			lock (envelopes) {
				if (envelopes == null)
					throw new DataException("Connection to server closed");

				while (true) {
					for (int i = 0; i < envelopes.Count; ++i) {
						var envelope = envelopes[i];
						if (ShouldReceive(senderMetadata, envelope)) {
							envelopes.RemoveAt(i);
							return envelope;
						}
					}

					// Return null if we haven't received a response input the timeout
					// period.
					if (timeout != 0 &&
					    DateTime.Now > timeOutHigh) {
						return null;
					}

					// Wait a second.
					try {
						Monitor.Wait(envelopes, 1000);
					} catch (ThreadInterruptedException) {
						/* ignore */
					}

				} // while (true)
			}
		}

		private void ReceiveEnvelopes() {
			try {
				while (CurrentState != ConnectorState.Closed) {
					var envelope = ReceiveEnvelope(0);
					lock (envelopes) {
						envelopes.Add(envelope);

						Monitor.PulseAll(envelopes);
					}
				}
			} catch (Exception) {
			} finally {
				// Invalidate this object when the thread finishes.
				object oldEnvelopes = envelopes;
				lock (oldEnvelopes) {
					envelopes = null;
					Monitor.PulseAll(oldEnvelopes);
				}
			}
		}

		private List<NetworkTriggerChannel> triggerChannels;

		#region NetworkTriggerChannel

		class NetworkTriggerChannel : ITriggerChannel {
			private NetworkClientConnector connector;

			public NetworkTriggerChannel(NetworkClientConnector connector, string triggerName, string objectName, TriggerEventType eventType) {
				this.connector = connector;
				TriggerName = triggerName;
				ObjectName = objectName;
				EventType = eventType;
			}

			public string TriggerName { get; private set; }

			public string ObjectName { get; private set; }

			public TriggerEventType EventType { get; private set; }

			public Action<TriggerEventNotification> OnInvoke { get; private set; }

			public void Dispose() {
				
			}

			public void OnTriggeInvoked(Action<TriggerEventNotification> notification) {
				if (OnInvoke != null) {
					OnInvoke = (Action<TriggerEventNotification>) Delegate.Combine(OnInvoke, notification);
				} else {
					OnInvoke = notification;
				}
			}
		}

		#endregion

		protected virtual void OnTriggerNotification(IMessageEnvelope envelope) {
			if (triggerChannels == null)
				return;

			lock (triggerChannels) {
				foreach (var channel in triggerChannels) {
				}
			}
		}

		private void DispatchTriggerCallbacks() {
			try {
				while (CurrentState != ConnectorState.Closed) {
					var notifications = new List<IMessageEnvelope>();
					
					lock (envelopes) {
						foreach (var envelope in envelopes) {
							if (envelope.Message is TriggerEventNotification) {
								notifications.Add(envelope);
							}
						}

						Monitor.PulseAll(envelopes);
					}

					foreach (var envelope in notifications) {
						OnTriggerNotification(envelope);
					}
				}
			} catch {
				
			}
		}

		#region ClientProcessor

		class ClientProcessor : IMessageProcessor {
			private readonly NetworkClientConnector connector;

			public ClientProcessor(NetworkClientConnector connector) {
				this.connector = connector;
			}

			public IMessageEnvelope ProcessMessage(IMessageEnvelope envelope) {
				var message = envelope.Message;
				IMessage response = null;

				if (message is ConnectRequest)
					response = RequestConnect(envelope);
				else if (message is AuthenticateRequest)
					response = RequestAuthenticate(envelope);
				else if (message is QueryExecuteRequest)
					response = RequestQueryExecute(envelope);
				else if (message is QueryResultPartRequest)
					response = RequestQueryResultPart(envelope);
				else if (message is DisposeResultRequest)
					response = RequestDisposeResult(envelope);
				else if (message is StreamableObjectCreateRequest)
					response = RequestCreateLargeObject(envelope);
				else if (message is StreamableObjectDisposeRequest)
					response = RequestDisposeLargeObject(envelope);
				else if (message is TriggerCreateRequest)
					response = RequestCreateTrigger(envelope);
				else if (message is BeginRequest)
					response = RequestBegin(envelope);
				else if (message is CommitRequest)
					response = RequestCommit(envelope);
				else if (message is RollbackRequest)
					response = RequestRollback(envelope);
				else if (message is PingRequest)
					response = Ping(envelope);
				else if (message is CloseCommand)
					response = RequestClose(envelope);
				
				if (response == null)
					throw new NotSupportedException();

				return CreateResponse(envelope.Metadata, response);
			}

			private IMessage Ping(IMessageEnvelope envelope) {
				return Request(envelope);
			}

			private IMessage RequestRollback(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestCommit(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestBegin(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestCreateTrigger(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestDisposeLargeObject(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestCreateLargeObject(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestDisposeResult(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestQueryResultPart(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestQueryExecute(IMessageEnvelope envelope) {
				throw new NotImplementedException();
			}

			private IMessage RequestAuthenticate(IMessageEnvelope envelope) {
				try {
					connector.AssertOpen();

					var response = Request(envelope);
					var responseMessage = (AuthenticateResponse) response;
					if (!responseMessage.Authenticated)
						throw new InvalidOperationException();

					connector.OnAuthenticated(((AuthenticateRequest)envelope.Message).UserName, responseMessage.TimeStamp);
					return response;
				} catch (Exception) {
					
					throw;
				}
			}

			private IMessage RequestClose(IMessageEnvelope envelope) {
				try {
					var response = Request(envelope);
					var responseMessage = (AcknowledgeResponse) response;
					if (!responseMessage.State)
						throw new InvalidOperationException();

					connector.Close();

					return response;
				} catch (Exception) {
					
					throw;
				}
			}

			private IMessage Request(IMessageEnvelope envelope) {
				connector.SendEnvelope(envelope);
				var response = connector.ReceiveResponse(connector.Timeout, envelope.Metadata);
				return connector.OnProcessServerResponse(response);
			}

			private IMessageEnvelope CreateResponse(IDictionary<string, object> senderMetadata, IMessage response) {
				return connector.CreateEnvelope(senderMetadata, response);
			}

			private IMessage RequestConnect(IMessageEnvelope envelope) {
				try {
					var request = (ConnectRequest) envelope.Message;
					connector.Timeout = request.Timeout;
					connector.OpenConnector(request.RemoteEndPoint);

					var response = Request(envelope);
					var responseMessage = (ConnectResponse) response;
					if (!responseMessage.Opened) {
						connector.Close();
						throw new InvalidOperationException();
					}

					return response;
				} catch (Exception) {
					//TODO: 
					throw;
				}
			}
		}

		#endregion
	}
}