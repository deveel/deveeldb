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
#if !PCL
using System.Security.Cryptography;
#endif

using Deveel.Data.Caching;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	public abstract class QueryContextBase : IQueryContext, IVariableScope {
#if PCL
		private Random secureRandom;
#else
		private RNGCryptoServiceProvider secureRandom;
#endif
		private ICache tableCache;
		private bool disposed;

		protected QueryContextBase() {
#if PCL
			secureRandom = new Random();
#else
			secureRandom = new RNGCryptoServiceProvider();
#endif
			tableCache = new MemoryCache(512, 1024, 30);
			VariableManager = new VariableManager(this);
		}

		~QueryContextBase() {
			Dispose(false);
		}


		public IDatabaseContext DatabaseContext {
			get { return Session.Database.DatabaseContext; }
		}

		public VariableManager VariableManager { get; private set; }

		public abstract IUserSession Session { get; }

		public virtual string CurrentSchema {
			get { return Session.CurrentSchema; }
		}

		public virtual ICache TableCache {
			get { return tableCache; }
		}

		public virtual IQueryContext ParentContext {
			get { return null; }
		}

		public virtual SqlNumber NextRandom(int bitSize) {
#if PCL
			var num = secureRandom.NextDouble();
#else
			var bytes = new byte[8];
			secureRandom.GetBytes(bytes);
			var num = BitConverter.ToInt64(bytes, 0);
#endif
			return new SqlNumber(num);
		}

		public void Dispose() {
			if (disposed)
				return;

			try {
				Dispose(true);
			} finally {
				tableCache = null;
				VariableManager = null;
				secureRandom = null;

				disposed = true;
			}
			
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {

		}

		DataObject IVariableResolver.Resolve(ObjectName variable) {
			throw new NotImplementedException();
		}

		DataType IVariableResolver.ReturnType(ObjectName variableName) {
			throw new NotImplementedException();
		}

		void IVariableScope.OnVariableDefined(Variable variable) {
			OnVariableDefined(variable);
		}

		void IVariableScope.OnVariableDropped(Variable variable) {
			OnVariableDropped(variable);
		}

		Variable IVariableScope.OnVariableGet(string name) {
			return OnVariableGet(name);
		}

		protected virtual Variable OnVariableGet(string name) {
			return null;
		}

		protected virtual void OnVariableDropped(Variable variable) {
		}

		protected virtual void OnVariableDefined(Variable variable) {
		}
	}
}