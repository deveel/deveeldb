// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Cursors {
	public static class ContextExtensions {
		public static bool DeclareCursor(this IContext context, CursorInfo cursorInfo, IRequest request) {
			if (context.CursorExists(cursorInfo.CursorName))
				throw new InvalidOperationException(String.Format("A cursor named '{0}' was already defined in the context.",
					cursorInfo.CursorName));

			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is ICursorScope) {
					var scope = (ICursorScope)currentContext;
					scope.CursorManager.DeclareCursor(cursorInfo, request);
					return true;
				}

				currentContext = currentContext.Parent;
			}

			return false;
		}

		public static void DeclareCursor(this IContext context, IRequest request, string cursorName, SqlQueryExpression query) {
			DeclareCursor(context, request, cursorName, (CursorFlags)0, query);
		}

		public static void DeclareCursor(this IContext context, IRequest request, string cursorName, CursorFlags flags, SqlQueryExpression query) {
			context.DeclareCursor(new CursorInfo(cursorName, flags, query), request);
		}

		public static void DeclareInsensitiveCursor(this IContext context, IRequest request, string cursorName, SqlQueryExpression query) {
			DeclareInsensitiveCursor(context, request, cursorName, query, false);
		}

		public static void DeclareInsensitiveCursor(this IContext context, IRequest request, string cursorName, SqlQueryExpression query, bool withScroll) {
			var flags = CursorFlags.Insensitive;
			if (withScroll)
				flags |= CursorFlags.Scroll;

			context.DeclareCursor(request, cursorName, flags, query);
		}


		public static bool CursorExists(this IContext context, string cursorName) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is ICursorScope) {
					var scope = (ICursorScope) currentContext;
					if (scope.CursorExists(cursorName))
						return true;
				}

				currentContext = currentContext.Parent;
			}

			return false;
		}

		public static Cursor FindCursor(this IContext context, string cursorName) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is ICursorScope) {
					var scope = (ICursorScope)currentContext;
					var cursor = scope.GetCursor(cursorName);
					if (cursor != null)
						return cursor;
				}

				currentContext = currentContext.Parent;
			}

			return null;
		}

		public static bool DropCursor(this IContext context, string cursorName) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is ICursorScope) {
					var scope = (ICursorScope)currentContext;
					if (scope.CursorExists(cursorName))
						return scope.DropCursor(cursorName);
				}

				currentContext = currentContext.Parent;
			}

			return false;
		}
	}
}
