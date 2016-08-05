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
using System.Collections.Generic;

using Deveel.Data.Sql.Variables;

namespace Deveel.Data {
	/// <summary>
	/// The context of a single execution block.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Blocks can be children of other blocks or of queries in the
	/// execution tree, and the context inherits from the query context
	/// or from the parent block context.
	/// </para>
	/// <para>
	/// A <see cref="BlockContext"/> is also a <see cref="IVariableScope"/> that
	/// means it holds <see cref="Variable">variables</see> for its entire
	/// lifetime, and dispose those defined during its existence at its disposal
	/// </para>
	/// </remarks>
	public sealed class BlockContext : Context, IBlockContext, IVariableScope, IExceptionInitScope {
		private VariableManager variableManager;
		private Dictionary<string, int> exceptions;

		internal BlockContext(IContext parent)
			: base(parent) {
			this.RegisterInstance<IBlockContext>(this);
			variableManager = new VariableManager(this);
			exceptions = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
		}

		IVariableManager IVariableScope.VariableManager {
			get { return variableManager; }
		}

		protected override string ContextName {
			get { return ContextNames.Block; }
		}

		public IBlockContext CreateBlockContext() {
			return new BlockContext(this);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (variableManager != null)
					variableManager.Dispose();
			}

			variableManager = null;
			base.Dispose(disposing);
		}

		void IExceptionInitScope.DeclareException(int errorCode, string exceptionName) {
			exceptions[exceptionName] = errorCode;
		}

		DeclaredException IExceptionInitScope.FindExceptionByName(string exceptionName) {
			int errorCode;
			if (!exceptions.TryGetValue(exceptionName, out errorCode))
				return null;

			return new DeclaredException(errorCode, exceptionName);
		}
	}
}
