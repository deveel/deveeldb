// 
//  Copyright 2010  Deveel
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

namespace Deveel.Data.Client {
	///<summary>
	/// The interface with the <see cref="Database"/> whether it be remotely via 
	/// TCP/IP or locally within the current runtime.
	///</summary>
	public interface IDatabaseInterface : IDisposable {
		///<summary>
		/// Attempts to log in to the database as the given username with the 
		/// given password.
		///</summary>
		/// <param name="database"></param>
		///<param name="default_schema"></param>
		///<param name="username"></param>
		///<param name="password"></param>
        ///<param name="call_back">A <see cref="IDatabaseCallBack"/> implementationthat 
        /// is notified of all events from the database. Events are only received if the 
        /// login was successful.</param>
		/// <remarks>
		/// Only one user may be authenticated per connection.
		/// <para>
        /// This must be called before the other methods are used.
		/// </para>
		/// </remarks>
		///<returns>
		/// Returns <b>true</b> if the authentication succeeded, otherwise false.
		/// </returns>
		bool Login(string default_schema, string username, string password, IDatabaseCallBack call_back);

		/// <summary>
		/// Changes the database of the current transaction to the interface.
		/// </summary>
		/// <param name="database">The name of the database to which to establish
		/// the current transaction.</param>
		void ChangeDatabase(string database);

	    ///<summary>
	    /// Pushes a part of a streamable object from the client onto the server.
	    ///</summary>
	    ///<param name="type">The <see cref="ReferenceType">type</see> of the object.</param>
	    ///<param name="object_id">The identifier of the <see cref="StreamableObject"/> 
	    /// for future queries.</param>
	    ///<param name="object_length">The total length of the <see cref="StreamableObject"/>.</param>
	    ///<param name="buf">The byte array representing the block of information being sent.</param>
	    ///<param name="offset">The offset into of the object of this block.</param>
	    ///<param name="length">The length of the block being pushed.</param>
	    /// <remarks>
	    /// The server stores the large object for use with a future command. 
	    /// For example,a sequence of with a command with large objects may operate as follows:
	    /// <list type="number">
	    /// <item>Push 100 MB object (id = 104)</item>
	    /// <item><see cref="ExecuteQuery"/> with command that contains a streamable object 
	    /// with id 104</item>
	    /// </list>
	    /// <para>
	    /// The client may push any part of a streamable object onto the server, 
	    /// however the streamable object must have been completely pushed for the 
	    /// command to execute correctly.  For example, an 100 MB byte array may be 
	    /// pushed onto the server in blocks of 64K (in 1,600 separate blocks).
	    /// </para>
	    /// </remarks>
	    void PushStreamableObjectPart(ReferenceType type, long object_id, long object_length,
	                                  byte[] buf, long offset, int length);

		///<summary>
		/// Executes the command and returns a <see cref="IQueryResponse"/> object that 
		/// describes the result of the command.
		///</summary>
		///<param name="sql"></param>
		/// <remarks>
		/// This method will block until the command has completed. The <see cref="IQueryResponse"/> 
		/// can be used to obtain the 'result id' variable that is used in subsequent 
		/// queries to the engine to retrieve the actual result of the command.
		/// </remarks>
		///<returns></returns>
		IQueryResponse ExecuteQuery(SqlQuery sql);

		///<summary>
        /// Returns a part of a result set.
		///</summary>
		///<param name="result_id"></param>
		///<param name="row_number"></param>
		///<param name="row_count"></param>
		/// <remarks>
		/// The result set part is referenced via the <see cref="IQueryResponse.ResultId">result id</see> 
		/// found in the <see cref="IQueryResponse"/>. This is used to Read parts of the command 
		/// once it has been found via <see cref="ExecuteQuery"/>.
		/// <para>
        /// If the result contains any <see cref="StreamableObject"/> objects, then the 
        /// server allocates a channel to the object via the <see cref="GetStreamableObjectPart"/> 
        /// and the identifier of the <see cref="StreamableObject"/>.  The channel may 
        /// only be disposed if the <see cref="DisposeStreamableObject"/> method is called.
		/// </para>
		/// </remarks>
		///<returns></returns>
		ResultPart GetResultPart(int result_id, int row_number, int row_count);

		///<summary>
        /// Disposes of a result of a command on the server.
		///</summary>
		///<param name="result_id"></param>
		/// <remarks>
		/// This frees up server side resources allocated to a command. This should be 
		/// called when the <see cref="ResultSet"/> of a command closes. We should try 
		/// and use this method as soon as possible because it frees locks on tables 
		/// and allows deleted rows to be reclaimed.
		/// </remarks>
		void DisposeResult(int result_id);

	    ///<summary>
	    /// Returns a section of a large binary or character stream in a result set.
	    ///</summary>
	    ///<param name="result_id"></param>
	    ///<param name="streamable_object_id"></param>
	    ///<param name="offset"></param>
	    ///<param name="len"></param>
	    /// <remarks>
	    /// This is used to stream large values over the connection.  For example, if a 
	    /// row contained a multi megabyte object and the client is only interested in 
	    /// the first few characters and the last few characters of the stream.
	    /// This would require only a few queries to the database and the multi-megabyte 
	    /// object would not need to be downloaded to the client in its entirety.
	    /// </remarks>
	    ///<returns></returns>
	    StreamableObjectPart GetStreamableObjectPart(int result_id, long streamable_object_id, long offset, int len);

		///<summary>
        /// Disposes a streamable object channel with the given identifier.
		///</summary>
		///<param name="result_id"></param>
		///<param name="streamable_object_id"></param>
		/// <remarks>
		/// This should be called to free any resources on the server associated with the
		/// object.  It should be called as soon as possible because it frees locks on the
		/// tables and allows deleted rows to be reclaimed.
		/// </remarks>
		void DisposeStreamableObject(int result_id, long streamable_object_id);
	}
}