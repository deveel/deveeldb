using System;
using System.Threading.Tasks;

using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Constraints {
	public interface IConstraint : IDbObject {
		ConstraintInfo ConstraintInfo { get; }

		Task AssertAsync(ITransaction transaction);
	}
}