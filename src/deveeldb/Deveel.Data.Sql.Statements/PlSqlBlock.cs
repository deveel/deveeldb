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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public class PlSqlBlock : CodeBlock, IPreparable {
		public PlSqlBlock() {
			Declarations = new DeclarationCollection();
			ExceptionHandlers = new ExceptionHandlerCollection();
		}

		protected PlSqlBlock(SerializationInfo info, StreamingContext context)
			: base(info, context) {
		}

		~PlSqlBlock() {
			Dispose(false);
		}

		public ICollection<SqlStatement> Declarations { get; private set; } 

		public ICollection<ExceptionHandler> ExceptionHandlers { get; private set; }

		protected virtual PlSqlBlock Prepare(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return Prepare(preparer);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (Declarations != null)
					Declarations.Clear();

				if (ExceptionHandlers != null)
					ExceptionHandlers.Clear();
			}

			Declarations = null;
			ExceptionHandlers = null;
		}

		#region DeclarationCollection

		class DeclarationCollection : Collection<SqlStatement> {
			private static void AssertDeclaration(SqlStatement statement) {
				if (!(statement is IDeclarationStatement))
					throw new ArgumentException(String.Format("The statement of type '{0}' is not a declaration.", statement.GetType()));
			}

			protected override void InsertItem(int index, SqlStatement item) {
				AssertDeclaration(item);
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, SqlStatement item) {
				AssertDeclaration(item);
				base.SetItem(index, item);
			}
		}

		#endregion

		#region ExceptionHandlerCollection

		class ExceptionHandlerCollection : Collection<ExceptionHandler> {
			private void AssertNotHandled(HandledExceptions handled) {
				foreach (var handler in base.Items) {
					if (handler.Handled.IsForOthers &&
						handled.IsForOthers)
						throw new ArgumentException("The OTHERS exception handler is already defined in the block.");
					foreach (var exceptionName in handled.ExceptionNames) {
						if (handler.Handles(exceptionName))
							throw new ArgumentException(String.Format("Trying to add a handler for exception '{0}' that is already handled.", exceptionName));
					}
				}
			}
			
			protected override void InsertItem(int index, ExceptionHandler item) {
				AssertNotHandled(item.Handled);
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, ExceptionHandler item) {
				AssertNotHandled(item.Handled);
				base.SetItem(index, item);
			}
		}

		#endregion
	}
}
