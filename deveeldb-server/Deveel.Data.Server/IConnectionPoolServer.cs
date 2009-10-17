using System;

namespace Deveel.Data.Server {
	/// <summary>
	/// An interface for the connection pool for a server.
	/// </summary>
	/// <remarks>
	/// This is the API for a service that accepts connections via 
	/// <see cref="AddConnection"/>, waits for the connection to make a 
	/// request, and dispatch the request as appropriate to the database 
	/// engine.
	/// <para>
	/// This interface is used to provide different implementations for 
	/// command dispatching mechanisms, such as a thread per TCP user, 
	/// one thread per TCP connection set, UDP, etc.
	/// </para>
	/// </remarks>
	interface IConnectionPoolServer {
		/// <summary>
		/// Connects a new <see cref="IServerConnection"/> into the pool 
		/// of connections to clients that this server maintains.
		/// </summary>
		/// <param name="connection"></param>
		void AddConnection(IServerConnection connection);

		/// <summary>
		/// Closes this connection pool server down.
		/// </summary>
		void Close();
	}
}