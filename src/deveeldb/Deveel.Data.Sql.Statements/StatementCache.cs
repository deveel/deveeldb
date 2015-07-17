using System;
using System.Collections.Generic;

using Deveel.Data.Caching;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// A wrapper around a specialized <see cref="ICache"/> used to
	/// store and retrieve parsed <see cref="SqlStatement"/> objects.
	/// </summary>
	public sealed class StatementCache {
		/// <summary>
		/// Constructs the object around the provided cache handler.
		/// </summary>
		/// <param name="cache">The <see cref="ICache"/> instance used to store the
		/// compiled statements.</param>
		public StatementCache(ICache cache) {
			Cache = cache;
		}

		public ICache Cache { get; private set; }

		public bool TryGet(string query, out IEnumerable<SqlStatement> statements) {
			throw new NotImplementedException();
		}

		public void Set(string query, IEnumerable<SqlStatement> statements) {
			throw new NotImplementedException();
		}
	}
}
