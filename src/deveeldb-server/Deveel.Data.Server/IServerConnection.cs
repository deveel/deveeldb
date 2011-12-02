using System;

namespace Deveel.Data.Server {
	/// <summary>
	/// A server side connection with a client.
	/// </summary>
	/// <remarks>
	/// Each client that is connected to the database has a 
	/// <see cref="IServerConnection"/> object.
	/// </remarks>
	interface IServerConnection {
		/// <summary>
		/// Gets the database this connection is oriented to.
		/// </summary>
		Database Database { get; }

		/// <summary>
		/// Determines if there is an entire command waiting to be serviced on this
		/// connection.
		/// </summary>
		/// <remarks>
		/// This method is always run on the same thread for all connections. It is 
		/// called many times a second by the connection pool server so it must execute 
		/// extremely fast.
		/// <para>
		/// <b>Issue</b>: Method is polled! Unfortunately can't get around this because 
		/// of the limitation that TCP connections must block on a thread, and we can't 
		/// block if we are to be servicing 100+ connections.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Return true if it has been determined that there is an entire command 
		/// waiting to be serviced on this connection.
		/// </returns>
		bool RequestPending();

		/// <summary>
		/// Processes a pending command on the connection.
		/// </summary>
		/// <remarks>
		/// This method is called from a database worker thread. The method will block 
		/// until a request has been received and processed. Note, it is not desirable 
		/// is some cases to allow this method to block. If a call to <see cref="RequestPending"/>
		/// returns true then then method is guarenteed not to block.
		/// <para>
		/// The first call to this method will handle the hand shaking protocol between 
		/// the client and server.
		/// </para>
		/// <para>
		/// While this method is doing something, it can not be called again even if another 
		/// request arrives from the client. All calls to this method are sequential. This 
		/// method will only be called if the <see cref="Ping"/> method is not currently being 
		/// processed.
		/// </para>
		/// </remarks>
		void ProcessRequest();

		/// <summary>
		/// Blocks until a complete command is available to be processed.
		/// </summary>
		/// <remarks>
		/// This is used for a blocking implementation. As soon as this method returns then a 
		/// call to <see cref="ProcessRequest"/> will process the incoming command.
		/// </remarks>
		void BlockForRequest();

		/// <summary>
		/// Pings the connection, to determine if it is alive.
		/// </summary>
		/// <remarks>
		/// This method will only be called if the <see cref="ProcessRequest"/> method is not 
		/// being processed.
		/// </remarks>
		/// <exception cref="System.IO.IOException">
		/// If the connection is not alive.
		/// </exception>
		void Ping();

		/// <summary>
		/// Closes this connection.
		/// </summary>
		void Close();
	}
}