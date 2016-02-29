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
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public class PlSqlBlock : ISqlCodeObject, IPreparable, IDisposable {
		private ICollection<ISqlCodeObject> objects;
		private ICollection<IStatement> declarations; 
		private ICollection<ExceptionHandler> exceptionHandlers;

		public PlSqlBlock() {
			objects = new List<ISqlCodeObject>();
			declarations = new List<IStatement>();
			exceptionHandlers = new List<ExceptionHandler>();
		}

		~PlSqlBlock() {
			Dispose(false);
		}

		public string Label { get; set; }

		public IEnumerable<IStatement> Declarations {
			get { return declarations.AsEnumerable(); }
		} 

		public IEnumerable<ExceptionHandler> ExceptionHandlers {
			get { return exceptionHandlers.AsEnumerable(); }
		}

		public IEnumerable<ISqlCodeObject> ChildObjects {
			get { return objects.AsEnumerable(); }
		}

		public void AddChild(ISqlCodeObject obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			if (obj is IStatement) {
				if (!(obj is IPlSqlStatement))
					throw new ArgumentException(String.Format("The statement of type '{0}' is not allowed in a PL/SQL block.", obj.GetType()));
			}

			objects.Add(obj);
		}


		public void AddExceptionHandler(ExceptionHandler handler) {
			// TODO: make further checks here ...
			exceptionHandlers.Add(handler);
		}


		protected virtual PlSqlBlock Prepare(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return Prepare(preparer);
		}

		public void Execute(ExecutionContext context) {
			
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (objects != null)
					objects.Clear();
				if (exceptionHandlers != null)
					exceptionHandlers.Clear();
			}

			objects = null;
			exceptionHandlers = null;
		}
	}
}
