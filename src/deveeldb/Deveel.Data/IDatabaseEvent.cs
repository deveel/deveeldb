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

namespace Deveel.Data {
	/// <summary>
	/// A generic interface for the implementation of an event to
	/// execute within a <see cref="Database"/> context.
	/// </summary>
	/// <remarks>
	/// Implementations of this interface will be passed to the method
	/// <see cref="Database.CreateEvent"/> to generate an event object.
	/// <para>
	/// The method <see cref="Execute"/> will be called by the system
	/// at the time of firing the event.
	/// </para>
	/// </remarks>
	public interface IDatabaseEvent {
		/// <summary>
		/// Executes the event within the database context.
		/// </summary>
		void Execute();
	}
}
