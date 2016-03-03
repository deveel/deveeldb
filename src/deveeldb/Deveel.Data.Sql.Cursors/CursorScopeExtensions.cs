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


using System;

namespace Deveel.Data.Sql.Cursors {
	public static class CursorScopeExtensions {
		public static void DeclareCursor(this ICursorScope scope, CursorInfo cursorInfo) {
			scope.CursorManager.DeclareCursor(cursorInfo);
		}

		public static Cursor GetCursor(this ICursorScope scope, string cursorName) {
			return scope.CursorManager.GetCursor(cursorName);
		}

		public static bool CursorExists(this ICursorScope scope, string cursorName) {
			return scope.CursorManager.CursorExists(cursorName);
		}

		public static bool DropCursor(this ICursorScope scope, string cursorName) {
			return scope.CursorManager.DropCursor(cursorName);
		}
	}
}
