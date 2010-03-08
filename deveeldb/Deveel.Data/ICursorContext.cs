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
	/// The context where cursors are declared and disposed.
	/// </summary>
	internal interface ICursorContext {
		/// <summary>
		/// Callback method invoked when a cursor has been 
		/// instanttiated.
		/// </summary>
		/// <param name="cursor">The reference to the cursor 
		/// instantiated.</param>
		void OnCursorCreated(Cursor cursor);

		/// <summary>
		/// Callback method invoked before a cursor is disposed.
		/// </summary>
		/// <param name="cursor">The reference to the cursor 
		/// that is being disposed.</param>
		void OnCursorDisposing(Cursor cursor);
	}
}