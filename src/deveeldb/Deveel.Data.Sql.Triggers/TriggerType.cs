// 
//  Copyright 2010-2015 Deveel
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


namespace Deveel.Data.Sql.Triggers {
	/// <summary>
	/// Enumerates the types of triggers, that can be volatile
	/// (like the <see cref="Callback"/>) or stored in the database.
	/// </summary>
	public enum TriggerType {
		/// <summary>
		/// A trigger that exists only within a user session
		/// and notifies of an event directly to the user client.
		/// </summary>
		Callback = 1,

		/// <summary>
		/// Triggers that define a procedural body stored in the
		/// database system.
		/// </summary>
		Procedure = 2
	}
}