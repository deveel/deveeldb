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
using System.Security.Cryptography;

using Deveel.Data.Caching;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.DbSystem {
	public abstract class QueryContextBase : IQueryContext {
		private readonly RNGCryptoServiceProvider secureRandom;
		private readonly ICache tableCache;
		private bool disposed;
		private Exception exception;

		protected QueryContextBase() {
			secureRandom = new RNGCryptoServiceProvider();
			tableCache = new MemoryCache(512, 1024, 30);
		}

		~QueryContextBase() {
			Dispose(false);
		}

		public abstract User User { get; }

		public abstract ISystemContext SystemContext { get; }

		public virtual ICache TableCache {
			get { return tableCache; }
		}

		public abstract IQueryPlanContext QueryPlanContext { get; }

		public bool IsExceptionState {
			get { return exception != null; }
		}

		public virtual IRoutineResolver RoutineResolver {
			get { return SystemContext.RoutineResolver; }
		}

		public virtual SqlNumber NextRandom(int bitSize) {
			var bytes = new byte[8];
			secureRandom.GetBytes(bytes);
			var num = BitConverter.ToInt64(bytes, 0);
			return new SqlNumber(num);
		}

		public abstract SqlNumber GetNextValue(ObjectName sequenceName);

		public abstract SqlNumber GetCurrentValue(ObjectName sequenceName);

		public abstract void SetCurrentValue(ObjectName sequenceName, SqlNumber value);

		public void SetExceptionState(Exception exception) {
			throw new NotImplementedException();
		}

		public Exception GetException() {
			return exception;
		}

		public void Dispose() {
			if (disposed)
				return;

			try {
				Dispose(true);
			} finally {
				disposed = true;
			}
			
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
		}
	}
}