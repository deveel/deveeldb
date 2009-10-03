// 
//  IDatabaseCallBack.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Client {
	///<summary>
	/// An interface that is input to the IDatabaseInterface as a way to be
	/// notified of event information from inside the database.
	///</summary>
	public interface IDatabaseCallBack {
		///<summary>
		/// Called when the database has generated an event that this user is 
		/// listening for.
		///</summary>
		///<param name="event_type"></param>
		///<param name="event_message"></param>
		/// <remarks>
		/// The thread that calls back these events is always a volatile thread 
		/// that may not block.  It is especially important that no queries are 
		/// executed when this calls back.  To safely act on events, it is advisable 
		/// to dispatch onto another thread.
		/// </remarks>
		void OnDatabaseEvent(int event_type, String event_message);
	}
}